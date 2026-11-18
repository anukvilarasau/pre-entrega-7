"""
app/main.py — API de producción del asesor de viajes multi-agente (FastAPI, 100% async).

Endpoints:
    POST /tasks                 encola una pregunta y devuelve job_id al instante (202)
    GET  /tasks/{job_id}        estado, progreso, pausa HITL, resultado o error
    POST /tasks/{job_id}/approve  responde la pausa HITL: {"aprobado": true|false}
    GET  /tasks                 los últimos jobs (resumen)
    GET  /health                Redis, checkpointer, modo, LangSmith y jobs activos

Ningún endpoint espera al grafo: POST /tasks guarda el job en Redis (redis.asyncio)
y le pasa el trabajo al worker (una tarea de asyncio). Todo lo que es sync y lento
(crear ChromaDB y los modelos al arrancar) va a un hilo con run_in_threadpool.

Levantar:
    uvicorn app.main:app --port 8000                  # usa REDIS_URL del .env
    MODO_MOCK=1 uvicorn app.main:app --port 8000      # sin claves (LLM con guion + Tavily falso + fakeredis)
"""

import logging
import os
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request, status
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import RedirectResponse
from langgraph.types import Command
from pydantic import BaseModel, ConfigDict, Field

import config
from app import observability
from app.graph import abrir_checkpointer, construir_grafo_api
from app.hitl import RespuestaAprobacion
from app.worker import EstadoJob, JobStore, Worker
from graph import Componentes, crear_componentes
from state import ahora

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuración de la API (variables de entorno)
# ---------------------------------------------------------------------------
@dataclass
class Ajustes:
    modo_mock: bool = config.MODO_MOCK
    redis_url: str = field(default_factory=lambda: os.getenv("REDIS_URL", "").strip())
    max_concurrentes: int = field(default_factory=lambda: int(os.getenv("MAX_JOBS_CONCURRENTES", "5")))
    timeout_s: float = field(default_factory=lambda: float(os.getenv("JOB_TIMEOUT_S", "900")))
    # Tope de conexiones a Redis (Redis Cloud gratis admite 30 clientes): 1/3 para los jobs, el resto checkpoints.
    redis_max_conexiones: int = field(default_factory=lambda: int(os.getenv("REDIS_MAX_CONEXIONES", "20")))
    guardar_trazas: bool | None = None  # None = solo en modo real (traces/api/)

    @classmethod
    def desde_entorno(cls) -> "Ajustes":
        modo = os.getenv("MODO_MOCK", "0").strip().lower() in {"1", "true", "si", "sí", "yes"}
        return cls(modo_mock=modo)


# ---------------------------------------------------------------------------
# Esquemas de entrada y salida
# ---------------------------------------------------------------------------
class TaskRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pregunta: str = Field(min_length=5, max_length=2000, description="El pedido de viaje, en castellano.",
                          examples=["Quiero ir 7 días a Bariloche en enero, somos 2, salimos de Mendoza y tenemos "
                                    "800.000 pesos. ¿Me armás el viaje y me decís si nos alcanza?"])
    auto_aprobar: bool = Field(default=False, description="true: aprueba la búsqueda web sin pausar (prueba de carga).")


class TaskCreada(BaseModel):
    job_id: str
    estado: EstadoJob
    links: dict[str, str]


class JobInfo(BaseModel):
    """Lo que devuelve GET /tasks/{job_id}."""

    job_id: str
    estado: EstadoJob
    pregunta: str
    auto_aprobar: bool = False
    modo: str
    thread_id: str
    creado: str
    iniciado: str | None = None
    actualizado: str | None = None
    terminado: str | None = None
    nodo_actual: str | None = None
    ruta: list[str] = []
    aprobacion_pendiente: dict[str, Any] | None = Field(
        default=None, description="Con estado WAITING_APPROVAL: qué quiere buscar en la web el investigador.")
    aprobaciones: list[dict[str, Any]] = []
    resultado: dict[str, Any] | None = None
    error: str | None = None
    tramos: int = 0
    duracion_s: float | None = Field(default=None, description="De la creación al final (incluye la espera humana).")
    tiempo_ejecucion_s: float | None = Field(default=None, description="Solo el tiempo que corrió el grafo.")
    total_tokens: int = 0
    costo_usd: float | None = Field(default=None, description="Costo estimado con los precios de app/observability.py.")
    tokens: dict[str, Any] = {}


class AprobacionAceptada(BaseModel):
    job_id: str
    estado: EstadoJob
    aprobado: bool


def _job_info(job: dict[str, Any]) -> JobInfo:
    return JobInfo(job_id=job["id"], **{k: v for k, v in job.items() if k != "id" and v is not None})


# ---------------------------------------------------------------------------
# Arranque y apagado
# ---------------------------------------------------------------------------
async def _conectar_redis(ajustes: Ajustes) -> tuple[Any, str]:
    if ajustes.redis_url:
        from redis.asyncio import BlockingConnectionPool, Redis

        pool = BlockingConnectionPool.from_url(ajustes.redis_url, decode_responses=True, timeout=60,
                                               max_connections=max(4, ajustes.redis_max_conexiones // 3))
        cliente = Redis(connection_pool=pool, auto_close_connection_pool=True)
        await cliente.ping()
        return cliente, "redis"
    import fakeredis

    log.warning("Sin REDIS_URL: el estado de los jobs vive en fakeredis (en memoria, se pierde al reiniciar).")
    return fakeredis.FakeAsyncRedis(decode_responses=True), "fakeredis"


def create_app(ajustes: Ajustes | None = None, componentes: Componentes | None = None) -> FastAPI:
    """Fábrica de la app (los tests le pasan ajustes y componentes propios)."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        a = ajustes or Ajustes.desde_entorno()
        config.configurar_logs(logging.INFO)
        config.validar_entorno(a.modo_mock)
        app.state.langsmith = observability.configurar_langsmith(a.modo_mock)
        async with AsyncExitStack() as pila:
            redis, tipo_redis = await _conectar_redis(a)
            pila.push_async_callback(redis.aclose)
            checkpointer = await pila.enter_async_context(abrir_checkpointer(
                a.redis_url or None, max_conexiones=max(4, a.redis_max_conexiones - a.redis_max_conexiones // 3)))
            # Chroma y los clientes de los modelos se crean con código sync: fuera del event loop.
            comp = componentes or await run_in_threadpool(crear_componentes, modo_mock=a.modo_mock)
            grafo = construir_grafo_api(comp, checkpointer)
            store = JobStore(redis)
            worker = Worker(grafo, store, modo_mock=a.modo_mock, max_concurrentes=a.max_concurrentes,
                            timeout_s=a.timeout_s,
                            guardar_trazas=(not a.modo_mock) if a.guardar_trazas is None else a.guardar_trazas)
            app.state.ajustes, app.state.store, app.state.worker, app.state.grafo = a, store, worker, grafo
            app.state.redis, app.state.tipo_redis = redis, tipo_redis
            app.state.checkpointer = type(checkpointer).__name__
            recuperados = await worker.recuperar(checkpoint_persistente=bool(a.redis_url))
            if recuperados:
                log.warning("Jobs recuperados al arrancar: %s", recuperados)
            log.info("API lista · modo %s · %s · %s", "mock" if a.modo_mock else "real", tipo_redis,
                     app.state.checkpointer)
            try:
                yield
            finally:
                await worker.cerrar()
                await observability.esperar_envio()

    app = FastAPI(
        title="Asesor de viajes multi-agente — API (Pre-entrega 7)",
        version="7.0.0",
        description="Orquestador LangGraph de la PE6 expuesto como API asíncrona con Redis, "
                    "human-in-the-loop y trazas en LangSmith.",
        lifespan=lifespan,
    )

    def _store(request: Request) -> JobStore:
        return request.app.state.store

    @app.get("/", include_in_schema=False)
    async def raiz() -> RedirectResponse:
        return RedirectResponse("/docs")

    @app.post("/tasks", status_code=status.HTTP_202_ACCEPTED, response_model=TaskCreada)
    async def crear_task(cuerpo: TaskRequest, request: Request) -> TaskCreada:
        """Guarda el job (PENDING) y lo encola. Vuelve enseguida: el grafo corre en segundo plano."""
        a: Ajustes = request.app.state.ajustes
        store = _store(request)
        job_id = await store.crear(cuerpo.pregunta.strip(), cuerpo.auto_aprobar, "mock" if a.modo_mock else "real")
        await request.app.state.worker.iniciar(job_id, cuerpo.pregunta, cuerpo.auto_aprobar)
        return TaskCreada(job_id=job_id, estado=EstadoJob.PENDING,
                          links={"estado": f"/tasks/{job_id}", "aprobar": f"/tasks/{job_id}/approve"})

    @app.get("/tasks/{job_id}", response_model=JobInfo)
    async def ver_task(job_id: str, request: Request) -> JobInfo:
        job = await _store(request).obtener(job_id)
        if job is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, f"No existe el job {job_id}.")
        return _job_info(job)

    @app.post("/tasks/{job_id}/approve", status_code=status.HTTP_202_ACCEPTED, response_model=AprobacionAceptada)
    async def aprobar_task(job_id: str, cuerpo: RespuestaAprobacion, request: Request) -> AprobacionAceptada:
        """Responde la pausa HITL y reanuda el grafo desde su checkpoint con Command(resume=...)."""
        store = _store(request)
        job = await store.obtener(job_id)
        if job is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, f"No existe el job {job_id}.")
        pendiente = job.get("aprobacion_pendiente")
        aprobaciones = [*(job.get("aprobaciones") or []),
                        {"ronda": (pendiente or {}).get("ronda"), "aprobado": cuerpo.aprobado,
                         "comentario": cuerpo.comentario, "temas": (pendiente or {}).get("temas_sin_memoria", []),
                         "timestamp": ahora()}]
        cambio = await store.transicion(job_id, {EstadoJob.WAITING_APPROVAL}, EstadoJob.RUNNING,
                                        aprobacion_pendiente=None, aprobaciones=aprobaciones)
        if not cambio:
            raise HTTPException(status.HTTP_409_CONFLICT,
                                f"El job {job_id} no está esperando aprobación (estado: {job['estado']}).")
        request.app.state.worker.lanzar(job_id, Command(resume=cuerpo.model_dump()))
        return AprobacionAceptada(job_id=job_id, estado=EstadoJob.RUNNING, aprobado=cuerpo.aprobado)

    @app.get("/tasks", response_model=list[JobInfo])
    async def listar_tasks(request: Request, limite: int = Query(20, ge=1, le=200)) -> list[JobInfo]:
        return [_job_info(j) for j in await _store(request).listar(limite)]

    @app.get("/health")
    async def health(request: Request) -> dict[str, Any]:
        s = request.app.state
        try:
            redis_ok = bool(await s.redis.ping())
        except Exception as e:  # noqa: BLE001
            redis_ok = False
            log.warning("health: Redis no responde: %s", e)
        return {
            "status": "ok" if redis_ok else "degradado",
            "modo": "mock" if s.ajustes.modo_mock else "real",
            "modelo": config.ANTHROPIC_MODEL,
            "redis": {"ok": redis_ok, "tipo": s.tipo_redis},
            "checkpointer": s.checkpointer,
            "langsmith": s.langsmith,
            "jobs_activos": s.worker.activos,
            "max_concurrentes": s.ajustes.max_concurrentes,
        }

    return app


app = create_app()
