"""Tests de la API (app/main.py) y del worker (app/worker.py): sin claves, con fakeredis e InMemorySaver.

Cubren lo que pide la consigna: POST devuelve job_id al instante, GET refleja el estado,
toda excepción termina en FAILED, pausa y reanudación del HITL (aprobado y rechazado),
y persistencia en Redis (un worker nuevo reanuda un job pausado por otro).
Si existe la variable REDIS_URL_TESTS (Redis 8 / Redis Stack), además se prueba AsyncRedisSaver real.
"""

import asyncio
import os
import time
from collections.abc import AsyncIterator
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import fakeredis
import httpx
import pytest
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command

import config
from app import observability
from app.graph import abrir_checkpointer, construir_grafo_api
from app.main import Ajustes, create_app
from app.worker import EstadoJob, JobStore, Worker
from graph import crear_componentes
from state import Aporte, DecisionRegistrada, OrquestadorState, PedidoViaje

DEMO = ("Quiero ir 7 días a Bariloche en enero, somos 2 personas, salimos desde Mendoza y tenemos "
        "800.000 pesos. ¿Me armás el viaje y me decís si nos alcanza?")
CLIMA = "¿Qué clima hace en Bariloche en julio?"


@pytest.fixture(autouse=True)
def hoy_fijo(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(config, "FECHA_REFERENCIA", date(2026, 11, 5))
    monkeypatch.setattr(config, "TRACES_DIR", tmp_path / "traces")


@pytest.fixture
async def api(tmp_path: Path) -> AsyncIterator[SimpleNamespace]:
    comp = crear_componentes(modo_mock=True, ruta_memoria=tmp_path / "chroma")
    app = create_app(Ajustes(modo_mock=True, redis_url="", timeout_s=30), componentes=comp)
    async with app.router.lifespan_context(app):
        transporte = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transporte, base_url="http://test") as cliente:
            yield SimpleNamespace(app=app, c=cliente, comp=comp)


async def esperar(c: httpx.AsyncClient, job_id: str, estados: set[str], limite_s: float = 15) -> dict[str, Any]:
    """Polling como haría un cliente; falla si el job no llega a ninguno de `estados`."""
    fin = time.monotonic() + limite_s
    while time.monotonic() < fin:
        job = (await c.get(f"/tasks/{job_id}")).json()
        if job["estado"] in estados:
            return job
        await asyncio.sleep(0.02)
    raise AssertionError(f"el job {job_id} no llegó a {estados}: quedó en {job['estado']}")


def grafo_de_prueba(checkpointer: Any, *, dormir: float = 0.0, error: Exception | None = None) -> Any:
    """Un grafo mínimo con el mismo estado que el real, para simular demoras y fallas."""
    async def sintesis(_s: OrquestadorState) -> dict[str, Any]:
        await asyncio.sleep(dormir)
        if error is not None:
            raise error
        return {"respuesta_final": "listo"}

    g = StateGraph(OrquestadorState)
    g.add_node("sintesis", sintesis)
    g.add_edge(START, "sintesis")
    g.add_edge("sintesis", END)
    return g.compile(checkpointer=checkpointer)


# ---------------------------------------------------------------------------
# POST /tasks y GET /tasks/{id}
# ---------------------------------------------------------------------------
async def test_post_devuelve_job_id_al_instante_sin_esperar_al_grafo(api: SimpleNamespace) -> None:
    api.app.state.worker.grafo = grafo_de_prueba(InMemorySaver(), dormir=1.0)
    t0 = time.perf_counter()
    r = await api.c.post("/tasks", json={"pregunta": DEMO})
    demora = time.perf_counter() - t0
    assert r.status_code == 202 and demora < 0.3, demora
    cuerpo = r.json()
    assert len(cuerpo["job_id"]) == 32 and cuerpo["estado"] == "PENDING"
    assert cuerpo["links"]["aprobar"] == f"/tasks/{cuerpo['job_id']}/approve"
    # mientras el grafo "trabaja", la API sigue respondiendo (el event loop no está bloqueado)
    t0 = time.perf_counter()
    assert (await api.c.get("/health")).status_code == 200 and time.perf_counter() - t0 < 0.3
    assert (await api.c.get(f"/tasks/{cuerpo['job_id']}")).json()["estado"] in {"PENDING", "RUNNING"}
    job = await esperar(api.c, cuerpo["job_id"], {"DONE"})
    assert job["resultado"]["respuesta_final"] == "listo" and job["duracion_s"] >= 1.0


async def test_get_refleja_cada_estado_del_job_con_hitl(api: SimpleNamespace) -> None:
    job_id = (await api.c.post("/tasks", json={"pregunta": DEMO})).json()["job_id"]
    pausado = await esperar(api.c, job_id, {"WAITING_APPROVAL"})
    assert pausado["nodo_actual"] == "aprobacion_web" and pausado["ruta"] == ["supervisor"]
    assert pausado["aprobacion_pendiente"]["temas_sin_memoria"] and pausado["iniciado"]
    assert pausado["thread_id"] == job_id and pausado["tramos"] == 1

    r = await api.c.post(f"/tasks/{job_id}/approve", json={"aprobado": True})
    assert r.status_code == 202 and r.json()["estado"] == "RUNNING"
    final = await esperar(api.c, job_id, {"DONE", "FAILED"})
    assert final["estado"] == "DONE" and final["error"] is None, final["error"]
    assert final["ruta"][:3] == ["supervisor", "aprobacion_web", "investigador"] and final["ruta"][-1] == "sintesis"
    assert final["resultado"]["permiso_web"] == "aprobada" and "## Itinerario" in final["resultado"]["respuesta_final"]
    assert final["resultado"]["metricas"]["busquedas_web"] > 0 and final["tramos"] == 2
    assert final["aprobacion_pendiente"] is None and final["aprobaciones"][0]["aprobado"] is True
    assert final["terminado"] and final["duracion_s"] >= final["tiempo_ejecucion_s"] > 0


async def test_approve_con_rechazo_termina_sin_web(api: SimpleNamespace) -> None:
    job_id = (await api.c.post("/tasks", json={"pregunta": DEMO})).json()["job_id"]
    await esperar(api.c, job_id, {"WAITING_APPROVAL"})
    await api.c.post(f"/tasks/{job_id}/approve", json={"aprobado": False, "comentario": "sin gastar"})
    final = await esperar(api.c, job_id, {"DONE", "FAILED"})
    assert final["estado"] == "DONE" and final["resultado"]["permiso_web"] == "rechazada"
    assert final["resultado"]["metricas"]["busquedas_web"] == 0
    assert "no fue autorizada" in final["resultado"]["respuesta_final"]
    assert api.comp.buscador.llamadas == []
    assert final["aprobaciones"][0]["comentario"] == "sin gastar"


async def test_auto_aprobar_no_se_detiene(api: SimpleNamespace) -> None:
    job_id = (await api.c.post("/tasks", json={"pregunta": CLIMA, "auto_aprobar": True})).json()["job_id"]
    final = await esperar(api.c, job_id, {"DONE", "FAILED", "WAITING_APPROVAL"})
    assert final["estado"] == "DONE" and final["tramos"] == 1 and "analista" not in final["ruta"]


async def test_errores_http(api: SimpleNamespace) -> None:
    assert (await api.c.get("/tasks/no-existe")).status_code == 404
    assert (await api.c.post("/tasks/no-existe/approve", json={"aprobado": True})).status_code == 404
    assert (await api.c.post("/tasks", json={"pregunta": "hola"})).status_code == 422  # muy corta
    assert (await api.c.post("/tasks", json={"pregunta": DEMO, "otro": 1})).status_code == 422
    job_id = (await api.c.post("/tasks", json={"pregunta": CLIMA, "auto_aprobar": True})).json()["job_id"]
    await esperar(api.c, job_id, {"DONE"})
    r = await api.c.post(f"/tasks/{job_id}/approve", json={"aprobado": True})
    assert r.status_code == 409 and "DONE" in r.json()["detail"]  # no estaba esperando aprobación


async def test_dos_approve_a_la_vez_solo_uno_reanuda(api: SimpleNamespace) -> None:
    job_id = (await api.c.post("/tasks", json={"pregunta": DEMO})).json()["job_id"]
    await esperar(api.c, job_id, {"WAITING_APPROVAL"})
    a, b = await asyncio.gather(api.c.post(f"/tasks/{job_id}/approve", json={"aprobado": True}),
                                api.c.post(f"/tasks/{job_id}/approve", json={"aprobado": False}))
    assert sorted([a.status_code, b.status_code]) == [202, 409]
    assert (await esperar(api.c, job_id, {"DONE", "FAILED"}))["estado"] == "DONE"


# ---------------------------------------------------------------------------
# FAILED: ninguna excepción deja al cliente en polling infinito
# ---------------------------------------------------------------------------
async def test_una_excepcion_del_grafo_pasa_el_job_a_failed(api: SimpleNamespace) -> None:
    api.app.state.worker.grafo = grafo_de_prueba(InMemorySaver(), error=RuntimeError("la API de Claude se cayó"))
    job_id = (await api.c.post("/tasks", json={"pregunta": DEMO})).json()["job_id"]
    job = await esperar(api.c, job_id, {"FAILED", "DONE"})
    assert job["estado"] == "FAILED" and job["error"] == "RuntimeError: la API de Claude se cayó"
    assert job["terminado"] and job["resultado"] is None


async def test_un_error_de_la_api_de_claude_queda_explicado(api: SimpleNamespace) -> None:
    import anthropic

    respuesta = httpx.Response(401, request=httpx.Request("POST", "https://api.anthropic.com/v1/messages"))
    error = anthropic.AuthenticationError("invalid x-api-key", response=respuesta, body=None)
    api.app.state.worker.grafo = grafo_de_prueba(InMemorySaver(), error=error)
    job_id = (await api.c.post("/tasks", json={"pregunta": DEMO})).json()["job_id"]
    job = await esperar(api.c, job_id, {"FAILED"})
    assert job["error"].startswith("AuthenticationError:") and "ANTHROPIC_API_KEY" in job["error"]


async def test_timeout_pasa_a_failed(api: SimpleNamespace) -> None:
    api.app.state.worker.grafo = grafo_de_prueba(InMemorySaver(), dormir=5)
    api.app.state.worker.timeout_s = 0.2
    job_id = (await api.c.post("/tasks", json={"pregunta": DEMO})).json()["job_id"]
    job = await esperar(api.c, job_id, {"FAILED"})
    assert job["error"].startswith("Timeout")


async def test_apagar_la_api_con_jobs_corriendo_los_deja_en_failed() -> None:
    redis = fakeredis.FakeAsyncRedis(decode_responses=True)
    store = JobStore(redis)
    worker = Worker(grafo_de_prueba(InMemorySaver(), dormir=10), store, modo_mock=True)
    job_id = await store.crear(DEMO, False, "mock")
    await worker.iniciar(job_id, DEMO, False)
    await asyncio.sleep(0.1)
    await worker.cerrar()
    job = await store.obtener(job_id)
    assert job is not None and job["estado"] == "FAILED" and job["error"].startswith("CancelledError")


# ---------------------------------------------------------------------------
# Persistencia en Redis y reanudación
# ---------------------------------------------------------------------------
async def test_el_estado_del_job_vive_en_un_hash_de_redis(api: SimpleNamespace) -> None:
    job_id = (await api.c.post("/tasks", json={"pregunta": CLIMA, "auto_aprobar": True})).json()["job_id"]
    final = await esperar(api.c, job_id, {"DONE"})
    redis = api.app.state.redis
    crudo = await redis.hgetall(f"job:{job_id}")
    assert crudo["estado"] == "DONE" and crudo["thread_id"] == job_id and crudo["pregunta"] == CLIMA
    for campo in ("creado", "iniciado", "terminado", "resultado", "tokens", "ruta"):
        assert crudo[campo], campo
    assert await redis.llen(f"job:{job_id}:eventos") == len(final["ruta"])
    assert await redis.zscore("jobs", job_id) is not None
    # otro JobStore (como si fuera otra instancia de la API) lee lo mismo
    assert (await JobStore(redis).obtener(job_id))["estado"] == "DONE"  # type: ignore[index]
    # y el checkpoint de LangGraph del mismo thread tiene el estado final del grafo
    snap = await api.app.state.grafo.aget_state({"configurable": {"thread_id": job_id}})
    assert snap.values["respuesta_final"] == final["resultado"]["respuesta_final"] and not snap.next


async def test_un_worker_nuevo_reanuda_el_job_pausado_por_otro(tmp_path: Path) -> None:
    """Simula un reinicio: el estado del job y el checkpoint quedan en Redis/checkpointer, no en el worker."""
    redis = fakeredis.FakeAsyncRedis(decode_responses=True)
    checkpointer = InMemorySaver()
    comp = crear_componentes(modo_mock=True, ruta_memoria=tmp_path / "chroma")

    store1 = JobStore(redis)
    worker1 = Worker(construir_grafo_api(comp, checkpointer), store1, modo_mock=True)
    job_id = await store1.crear(DEMO, False, "mock")
    await worker1.iniciar(job_id, DEMO, False)
    await worker1.esperar()
    assert (await store1.obtener(job_id))["estado"] == EstadoJob.WAITING_APPROVAL  # type: ignore[index]
    del worker1, store1

    store2 = JobStore(redis)
    worker2 = Worker(construir_grafo_api(comp, checkpointer), store2, modo_mock=True)
    assert await store2.transicion(job_id, {EstadoJob.WAITING_APPROVAL}, EstadoJob.RUNNING)
    worker2.lanzar(job_id, Command(resume={"aprobado": True}))
    await worker2.esperar()
    job = await store2.obtener(job_id)
    assert job is not None and job["estado"] == "DONE" and job["ruta"][1] == "aprobacion_web"


async def test_al_arrancar_sin_checkpoints_persistentes_los_huerfanos_pasan_a_failed() -> None:
    redis = fakeredis.FakeAsyncRedis(decode_responses=True)
    store = JobStore(redis)
    job_id = await store.crear(DEMO, False, "mock")
    await store.actualizar(job_id, estado=EstadoJob.RUNNING)  # la API "se cayó" con el job corriendo
    worker = Worker(grafo_de_prueba(InMemorySaver()), store, modo_mock=True)
    assert await worker.recuperar(checkpoint_persistente=False) == []
    job = await store.obtener(job_id)
    assert job is not None and job["estado"] == "FAILED" and "se reinició" in job["error"]


# ---------------------------------------------------------------------------
# Concurrencia, health y utilidades de observabilidad
# ---------------------------------------------------------------------------
async def test_cinco_pedidos_concurrentes_terminan_todos(api: SimpleNamespace) -> None:
    respuestas = await asyncio.gather(*[
        api.c.post("/tasks", json={"pregunta": DEMO if i % 2 == 0 else CLIMA, "auto_aprobar": True}) for i in range(5)
    ])
    ids = [r.json()["job_id"] for r in respuestas]
    assert len(set(ids)) == 5
    finales = await asyncio.gather(*[esperar(api.c, i, {"DONE", "FAILED"}, limite_s=30) for i in ids])
    assert [f["estado"] for f in finales] == ["DONE"] * 5, [f["error"] for f in finales]
    assert len((await api.c.get("/tasks?limite=10")).json()) == 5


async def test_health(api: SimpleNamespace) -> None:
    h = (await api.c.get("/health")).json()
    assert h["status"] == "ok" and h["modo"] == "mock" and h["redis"] == {"ok": True, "tipo": "fakeredis"}
    assert h["checkpointer"] == "InMemorySaver" and h["langsmith"]["tracing"] is False


def test_costo_y_percentiles() -> None:
    tokens = {"claude-haiku-5-5": {"input_tokens": 1_000_000, "output_tokens": 200_000, "total_tokens": 1_200_000,
                                   "input_token_details": {"cache_read": 500_000}}}
    assert observability.costo_usd(tokens) == pytest.approx(0.5 * 0.10 + 0.5 * 0.01 + 0.2 * 0.50)
    assert observability.costo_usd({"otro-modelo": {"input_tokens": 1}}) is None
    assert observability.percentil([1, 2, 3, 4, 5], 95) == pytest.approx(4.8)
    suma = observability.sumar_tokens(tokens, {"claude-haiku-5-5": {"input_tokens": 10, "input_token_details": {"cache_read": 1}}})
    assert suma["claude-haiku-5-5"]["input_tokens"] == 1_000_010
    assert suma["claude-haiku-5-5"]["input_token_details"]["cache_read"] == 500_001


def test_config_de_job_lleva_metadata_para_langsmith() -> None:
    cfg = observability.config_de_job("abc", modo_mock=False, tramo=2, callbacks=[])
    assert cfg["configurable"] == {"thread_id": "abc"}
    assert cfg["metadata"]["job_id"] == "abc" and cfg["metadata"]["tramo"] == 2 and "real" in cfg["tags"]


@pytest.mark.skipif(not os.getenv("REDIS_URL_TESTS"), reason="sin Redis 8 / Redis Stack (definí REDIS_URL_TESTS)")
async def test_pausa_y_reanudacion_con_async_redis_saver_real(tmp_path: Path) -> None:
    comp = crear_componentes(modo_mock=True, ruta_memoria=tmp_path / "chroma")
    thread = f"test-{time.time_ns()}"
    cfg = {"configurable": {"thread_id": thread}}
    from state import estado_inicial

    async with abrir_checkpointer(os.environ["REDIS_URL_TESTS"]) as saver:
        await construir_grafo_api(comp, saver).ainvoke(estado_inicial(DEMO), cfg)
    async with abrir_checkpointer(os.environ["REDIS_URL_TESTS"]) as saver:  # conexión nueva: "reinicio"
        grafo = construir_grafo_api(comp, saver)
        pausado = await grafo.aget_state(cfg)
        assert pausado.next == ("aprobacion_web",)
        # RedisSaver guarda JSON: los modelos del estado tienen que volver como modelos, no como dict
        assert isinstance(pausado.values["pedido"], PedidoViaje)
        assert all(isinstance(d, DecisionRegistrada) for d in pausado.values["decisiones"])
        final = await grafo.ainvoke(Command(resume={"aprobado": True}), cfg)
    assert final["permiso_web"] == "aprobada" and final["respuesta_final"]
    assert all(isinstance(a, Aporte) for a in final["aportes"]) and final["aportes"][0].fuentes
