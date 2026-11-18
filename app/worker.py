"""
app/worker.py — El estado de cada job en Redis y el worker que corre el grafo.

JobStore (Redis): un HASH por job, `job:<job_id>`, con
    pregunta, estado, auto_aprobar, modo, thread_id (= job_id), timestamps
    (creado, iniciado, actualizado, terminado), nodo_actual, ruta,
    aprobacion_pendiente, aprobaciones, resultado, error, tokens, costo_usd...
más una lista `job:<job_id>:eventos` (lo que hizo cada nodo, para la traza) y un
sorted set `jobs` (por fecha de creación, para GET /tasks).

Estados: PENDING → RUNNING → (WAITING_APPROVAL → RUNNING)* → DONE | FAILED.
Las transiciones que pueden competir (dos approve a la vez) usan WATCH/MULTI.

Worker: cada job es una tarea de asyncio (`asyncio.create_task`) cuya referencia
se guarda en un set hasta que termina (si no, el GC puede cortarla a la mitad).
Un semáforo limita cuántos grafos corren a la vez. TODA excepción del grafo
termina en FAILED con el error (tipo + mensaje): el cliente nunca queda haciendo
polling infinito. También hay un timeout por tramo.

El grafo se ejecuta con thread_id = job_id: si se pausa en el nodo HITL, el
checkpoint queda en Redis (AsyncRedisSaver) y POST /approve lo reanuda con
Command(resume=...), aunque la API se haya reiniciado en el medio.
"""

import asyncio
import json
import logging
import time
import uuid
from enum import StrEnum
from typing import Any

from langchain_core.callbacks import UsageMetadataCallbackHandler
from langgraph.errors import GraphRecursionError
from langgraph.graph.state import CompiledStateGraph
from langgraph.types import Command
from redis.asyncio import Redis
from redis.exceptions import WatchError

import config
from app import observability
from state import ahora, estado_inicial
from tracing import Evento, Traza

log = logging.getLogger(__name__)


class EstadoJob(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    WAITING_APPROVAL = "WAITING_APPROVAL"
    DONE = "DONE"
    FAILED = "FAILED"


TERMINALES: frozenset[str] = frozenset({EstadoJob.DONE, EstadoJob.FAILED})
CAMPOS_JSON: frozenset[str] = frozenset({"ruta", "aprobacion_pendiente", "aprobaciones", "resultado", "tokens"})
CAMPOS_NUMERICOS: frozenset[str] = frozenset({"costo_usd", "duracion_s", "tiempo_ejecucion_s"})
CAMPOS_ENTEROS: frozenset[str] = frozenset({"tramos", "total_tokens"})


def _a_redis(valor: Any) -> str:
    if isinstance(valor, bool):
        return "1" if valor else "0"
    if valor is None:
        return ""
    if isinstance(valor, (dict, list)):
        return json.dumps(valor, ensure_ascii=False, default=str)
    return str(valor)


def _segundos(desde: str, hasta: str) -> float:
    from datetime import datetime

    return round((datetime.fromisoformat(hasta) - datetime.fromisoformat(desde)).total_seconds(), 2)


class JobStore:
    """El estado de los jobs en Redis (todas las operaciones son async: no bloquean el event loop)."""

    def __init__(self, redis: Redis, prefijo: str = "job") -> None:
        self.redis = redis
        self.prefijo = prefijo

    def clave(self, job_id: str) -> str:
        return f"{self.prefijo}:{job_id}"

    async def crear(self, pregunta: str, auto_aprobar: bool, modo: str) -> str:
        job_id = uuid.uuid4().hex
        momento = ahora()
        async with self.redis.pipeline(transaction=True) as pipe:  # una sola ida y vuelta a Redis
            pipe.hset(self.clave(job_id), mapping={
                "id": job_id, "thread_id": job_id, "pregunta": pregunta, "estado": EstadoJob.PENDING,
                "auto_aprobar": _a_redis(auto_aprobar), "modo": modo, "creado": momento, "actualizado": momento,
                "ruta": "[]", "aprobaciones": "[]", "tokens": "{}", "tramos": "0",
            })
            pipe.zadd(f"{self.prefijo}s", {job_id: time.time()})
            await pipe.execute()
        return job_id

    async def obtener(self, job_id: str) -> dict[str, Any] | None:
        crudo = await self.redis.hgetall(self.clave(job_id))
        if not crudo:
            return None
        datos: dict[str, Any] = {}
        for k, v in crudo.items():
            k = k.decode() if isinstance(k, bytes) else k
            v = v.decode() if isinstance(v, bytes) else v
            if k in CAMPOS_JSON:
                datos[k] = json.loads(v) if v else None
            elif k in CAMPOS_NUMERICOS:
                datos[k] = float(v) if v else None
            elif k in CAMPOS_ENTEROS:
                datos[k] = int(v) if v else 0
            elif k == "auto_aprobar":
                datos[k] = v == "1"
            else:
                datos[k] = v or None
        return datos

    async def actualizar(self, job_id: str, **campos: Any) -> None:
        campos["actualizado"] = ahora()
        await self.redis.hset(self.clave(job_id), mapping={k: _a_redis(v) for k, v in campos.items()})

    async def transicion(self, job_id: str, desde: set[str], hacia: EstadoJob, **campos: Any) -> bool:
        """Cambia el estado SOLO si el actual está en `desde` (atómico con WATCH/MULTI). Devuelve si cambió."""
        clave = self.clave(job_id)
        async with self.redis.pipeline(transaction=True) as pipe:
            try:
                await pipe.watch(clave)
                actual = await pipe.hget(clave, "estado")
                actual = actual.decode() if isinstance(actual, bytes) else actual
                if actual not in desde:
                    await pipe.unwatch()
                    return False
                pipe.multi()
                campos.update(estado=hacia, actualizado=ahora())
                pipe.hset(clave, mapping={k: _a_redis(v) for k, v in campos.items()})
                await pipe.execute()
                return True
            except WatchError:  # otro request cambió el job en el medio
                return False

    async def agregar_evento(self, job_id: str, evento: dict[str, Any]) -> None:
        clave = self.clave(job_id)
        ruta = json.loads((await self.redis.hget(clave, "ruta")) or "[]")
        ruta.append(evento["nodo"])
        await self.redis.rpush(f"{clave}:eventos", json.dumps(evento, ensure_ascii=False, default=str))
        await self.actualizar(job_id, nodo_actual=evento["nodo"], ruta=ruta)

    async def eventos(self, job_id: str) -> list[dict[str, Any]]:
        return [json.loads(e) for e in await self.redis.lrange(f"{self.clave(job_id)}:eventos", 0, -1)]

    async def listar(self, limite: int = 20) -> list[dict[str, Any]]:
        ids = await self.redis.zrevrange(f"{self.prefijo}s", 0, max(0, limite - 1))
        jobs = [await self.obtener(i.decode() if isinstance(i, bytes) else i) for i in ids]
        return [j for j in jobs if j]

    async def en_estado(self, *estados: str) -> list[str]:
        ids = await self.redis.zrange(f"{self.prefijo}s", 0, -1)
        salida = []
        for i in ids:
            i = i.decode() if isinstance(i, bytes) else i
            estado = await self.redis.hget(self.clave(i), "estado")
            if (estado.decode() if isinstance(estado, bytes) else estado) in estados:
                salida.append(i)
        return salida


class Worker:
    """Corre el grafo de cada job en una tarea de asyncio de fondo."""

    def __init__(
        self,
        grafo: CompiledStateGraph,
        store: JobStore,
        *,
        modo_mock: bool,
        max_concurrentes: int = 5,
        timeout_s: float = 900.0,
        guardar_trazas: bool = False,
    ) -> None:
        self.grafo = grafo
        self.store = store
        self.modo_mock = modo_mock
        self.timeout_s = timeout_s
        self.guardar_trazas = guardar_trazas
        self._semaforo = asyncio.Semaphore(max_concurrentes)
        self._tareas: set[asyncio.Task[None]] = set()  # referencias fuertes hasta que cada tarea termina

    @property
    def activos(self) -> int:
        return len(self._tareas)

    def lanzar(self, job_id: str, entrada: dict[str, Any] | Command | None) -> asyncio.Task[None]:
        """Encola el job y vuelve enseguida (lo llama el endpoint; no espera al grafo)."""
        tarea = asyncio.create_task(self._ejecutar(job_id, entrada), name=f"job-{job_id}")
        self._tareas.add(tarea)
        tarea.add_done_callback(self._tareas.discard)
        return tarea

    async def iniciar(self, job_id: str, pregunta: str, auto_aprobar: bool) -> asyncio.Task[None]:
        return self.lanzar(job_id, estado_inicial(pregunta, auto_aprobar=auto_aprobar))

    async def esperar(self) -> None:
        """Espera a que terminen las tareas en curso (tests y prueba de carga en proceso)."""
        while self._tareas:
            await asyncio.gather(*list(self._tareas), return_exceptions=True)

    async def cerrar(self) -> None:
        """Al apagar la API: se cancelan las tareas; cada una deja su job en FAILED (no queda colgado)."""
        for t in list(self._tareas):
            t.cancel()
        await asyncio.gather(*list(self._tareas), return_exceptions=True)

    async def recuperar(self, checkpoint_persistente: bool) -> list[str]:
        """Al arrancar: jobs que quedaron RUNNING/PENDING porque la API se cayó sin apagarse bien.

        Con checkpoints en Redis se reanudan desde el último paso guardado (entrada None);
        sin checkpoints persistentes no hay de dónde seguir → FAILED con el motivo.
        Los WAITING_APPROVAL no se tocan: siguen esperando su approve.
        """
        recuperados = []
        for job_id in await self.store.en_estado(EstadoJob.PENDING, EstadoJob.RUNNING):
            job = await self.store.obtener(job_id) or {}
            cfg = {"configurable": {"thread_id": job_id}}
            hay_checkpoint = checkpoint_persistente and (await self.grafo.aget_state(cfg)).values
            if hay_checkpoint:
                log.warning("[worker] job %s: se reanuda desde su último checkpoint", job_id)
                self.lanzar(job_id, None)
            elif checkpoint_persistente:  # nunca llegó a correr: se arranca de cero
                self.lanzar(job_id, estado_inicial(job.get("pregunta") or "?", bool(job.get("auto_aprobar"))))
            else:
                await self._fallar(job_id, "La API se reinició mientras el job corría y no hay checkpoints "
                                           "persistentes (modo sin Redis real).")
                continue
            recuperados.append(job_id)
        return recuperados

    # ------------------------------------------------------------------
    async def _ejecutar(self, job_id: str, entrada: dict[str, Any] | Command | None) -> None:
        tokens = UsageMetadataCallbackHandler()  # afuera del try: si el tramo falla, sus tokens igual se cuentan
        try:
            async with self._semaforo:
                await asyncio.wait_for(self._correr_tramo(job_id, entrada, tokens), timeout=self.timeout_s)
        except asyncio.CancelledError:
            await asyncio.shield(self._fallar(job_id, "CancelledError: la API se apagó mientras el job corría.", tokens))
            raise
        except TimeoutError:
            await self._fallar(job_id, f"Timeout: el tramo superó los {self.timeout_s:.0f} s.", tokens)
        except Exception as e:  # noqa: BLE001 — toda excepción del grafo termina en FAILED
            log.exception("[worker] job %s falló", job_id)
            await self._fallar(job_id, _explicar(e), tokens)

    async def _fallar(self, job_id: str, error: str, tokens: UsageMetadataCallbackHandler | None = None) -> None:
        try:
            job = await self.store.obtener(job_id) or {}
            momento = ahora()
            extra: dict[str, Any] = {}
            if tokens is not None and tokens.usage_metadata:
                acumulados = observability.sumar_tokens(job.get("tokens") or {}, tokens.usage_metadata)
                extra = {"tokens": acumulados, "costo_usd": observability.costo_usd(acumulados),
                         "total_tokens": observability.total_tokens(acumulados)}
            await self.store.actualizar(job_id, estado=EstadoJob.FAILED, error=error, terminado=momento,
                                        aprobacion_pendiente=None, **extra,
                                        duracion_s=_segundos(job["creado"], momento) if job.get("creado") else None)
        except Exception:  # noqa: BLE001 — si ni Redis responde, al menos queda en el log
            log.exception("[worker] no se pudo marcar FAILED el job %s (error original: %s)", job_id, error)

    async def _correr_tramo(self, job_id: str, entrada: dict[str, Any] | Command | None,
                            tokens: UsageMetadataCallbackHandler) -> None:
        job = await self.store.obtener(job_id)
        if job is None:
            raise KeyError(f"El job {job_id} no existe en Redis.")
        tramo = int(job.get("tramos") or 0) + 1
        inicio_tramo = time.perf_counter()
        await self.store.actualizar(job_id, estado=EstadoJob.RUNNING, tramos=tramo, iniciado=job.get("iniciado") or ahora())

        cfg = observability.config_de_job(job_id, modo_mock=self.modo_mock, tramo=tramo, callbacks=[tokens])
        auxiliar = Traza(pregunta=job["pregunta"])  # solo para armar el detalle de cada evento igual que en la PE6
        cortado = False
        try:
            async for modo, dato in self.grafo.astream(entrada, cfg, stream_mode=["updates", "values"]):
                if modo != "updates":
                    continue
                for nodo, salida in dato.items():
                    if nodo.startswith("__"):  # "__interrupt__": la pausa se lee del estado más abajo
                        continue
                    auxiliar.registrar(nodo, salida or {})
                    e = auxiliar.eventos[-1]
                    await self.store.agregar_evento(job_id, {"nodo": e.nodo, "timestamp": e.timestamp,
                                                             "detalle": e.detalle, "tramo": tramo})
        except GraphRecursionError:
            cortado = True  # freno de LangGraph: se termina de forma controlada con lo que haya (igual que la PE6)

        job = await self.store.obtener(job_id) or job
        acumulados = observability.sumar_tokens(job.get("tokens") or {}, tokens.usage_metadata)
        ejecucion = round((job.get("tiempo_ejecucion_s") or 0) + time.perf_counter() - inicio_tramo, 2)
        comunes = {"tokens": acumulados, "costo_usd": observability.costo_usd(acumulados),
                   "total_tokens": observability.total_tokens(acumulados), "tiempo_ejecucion_s": ejecucion}

        snapshot = await self.grafo.aget_state({"configurable": {"thread_id": job_id}})
        if snapshot.interrupts and not cortado:
            pendiente = snapshot.interrupts[0].value
            log.info("[worker] job %s: pausa HITL, espera aprobación", job_id)
            await self.store.actualizar(job_id, estado=EstadoJob.WAITING_APPROVAL, nodo_actual="aprobacion_web",
                                        aprobacion_pendiente=pendiente, **comunes)
            return

        await self._terminar(job_id, job, dict(snapshot.values), cortado, comunes)

    async def _terminar(self, job_id: str, job: dict[str, Any], valores: dict[str, Any], cortado: bool,
                        comunes: dict[str, Any]) -> None:
        traza = Traza(pregunta=job["pregunta"], escenario=f"api_{job_id[:8]}",
                      modo="mock" if self.modo_mock else "real", inicio=job["creado"])
        for i, e in enumerate(await self.store.eventos(job_id), 1):
            traza.ruta.append(e["nodo"])
            traza.eventos.append(Evento(n=i, nodo=e["nodo"], timestamp=e["timestamp"], detalle=e["detalle"]))
        aviso = (f"Me detuve de forma controlada: el grafo llegó a su límite de {config.RECURSION_LIMIT} pasos "
                 "(RECURSION_LIMIT) antes de terminar.") if cortado else None
        traza.cerrar("cortado_por_recursion_limit" if cortado else "completo", valores, comunes["tokens"], aviso=aviso)
        if self.guardar_trazas:
            traza.guardar(config.TRACES_DIR / "api")
        resultado = {
            "respuesta_final": traza.respuesta_final,
            "estado_grafo": traza.estado,
            "ruta": traza.texto_ruta(),
            "permiso_web": valores.get("permiso_web", "libre"),
            "aprobaciones_hitl": [r.model_dump(mode="json") if hasattr(r, "model_dump") else r
                                  for r in valores.get("aprobaciones", [])],
            "metricas": traza.metricas,
            "pedido": traza.pedido,
            "traza": traza.archivo or None,
        }
        momento = ahora()
        await self.store.actualizar(job_id, estado=EstadoJob.DONE, resultado=resultado, terminado=momento,
                                    nodo_actual="", aprobacion_pendiente=None,
                                    duracion_s=_segundos(job["creado"], momento), **comunes)
        log.info("[worker] job %s DONE en %.1f s (%s)", job_id, _segundos(job["creado"], momento), traza.texto_ruta())


def _explicar(e: Exception) -> str:
    """Tipo + mensaje, con la traducción de main.py para los errores típicos de la API de Claude."""
    try:
        from main import explicar_error  # main.py de la PE6 (consola)

        texto = explicar_error(e)
    except Exception:  # noqa: BLE001
        texto = f"{type(e).__name__}: {e}"
    return texto if texto.startswith(type(e).__name__) else f"{type(e).__name__}: {texto}"
