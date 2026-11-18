"""
app/graph.py — El grafo de producción: el orquestador de la PE6 + HITL + checkpoints.

No se reescribe la topología: se reutiliza `graph.construir_grafo()` de la PE6
(supervisor con Literal, rúbrica R1–R6, frenos anti-bucle, especialistas ReAct)
y se le agregan dos cosas:

1. el nodo human-in-the-loop "aprobacion_web" (app/hitl.py) en el camino
   supervisor → investigador;
2. un checkpointer: AsyncRedisSaver (langgraph-checkpoint-redis) con Redis
   real, o InMemorySaver en modo mock / tests (fakeredis no implementa los
   comandos FT.* de RediSearch que necesita RedisSaver).

Cada job usa thread_id = job_id, así el checkpoint de un job pausado sobrevive
a un reinicio de la API y se reanuda con Command(resume=...).
"""

import logging
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
from langgraph.graph.state import CompiledStateGraph

from app.hitl import crear_nodo_aprobacion
from graph import Componentes, construir_grafo, crear_componentes, diagrama_mermaid

log = logging.getLogger(__name__)

# Los checkpoints de un job se borran solos después de este tiempo (Redis TTL, en minutos).
CHECKPOINT_TTL_MINUTOS: int = 7 * 24 * 60

# Tipos propios que viajan en el estado (y en los artifacts de las herramientas). Se declaran
# explícitamente: langgraph-checkpoint avisa que deserializar tipos no registrados se va a bloquear.
TIPOS_DEL_ESTADO: list[tuple[str, str]] = [
    ("state", nombre) for nombre in ("PedidoViaje", "Aporte", "Fuente", "LlamadaHerramienta", "CoberturaTema",
                                     "DecisionRegistrada", "RegistroAprobacion")
]


def construir_grafo_api(
    componentes: Componentes,
    checkpointer: BaseCheckpointSaver | None,
    max_pasos: int | None = None,
) -> CompiledStateGraph:
    """El grafo que corre la API: el de la PE6 + el nodo HITL + checkpointer."""
    return construir_grafo(
        componentes,
        max_pasos=max_pasos,
        nodo_aprobacion=crear_nodo_aprobacion(componentes.memoria),
        checkpointer=checkpointer,
    )


@asynccontextmanager
async def abrir_checkpointer(redis_url: str | None, max_conexiones: int = 14) -> AsyncIterator[BaseCheckpointSaver]:
    """AsyncRedisSaver si hay Redis real; InMemorySaver si no (modo mock / tests).

    El cliente de Redis usa un pool BLOQUEANTE con tope de conexiones: si los jobs
    concurrentes piden más conexiones que el tope, esperan una libre en lugar de fallar.
    (Sin tope, la primera prueba de carga contra Redis Cloud gratis -30 clientes- dio
    "max number of clients reached" en 2 de 5 jobs.)
    """
    if not redis_url:
        log.info("Checkpointer: InMemorySaver (sin Redis real: modo mock o tests)")
        yield InMemorySaver(serde=JsonPlusSerializer(allowed_msgpack_modules=TIPOS_DEL_ESTADO))
        return
    from langgraph.checkpoint.redis.aio import AsyncRedisSaver
    from langgraph.checkpoint.redis.jsonplus_redis import JsonPlusRedisSerializer
    from redis.asyncio import BlockingConnectionPool, Redis

    pool = BlockingConnectionPool.from_url(redis_url, max_connections=max_conexiones, timeout=60)
    cliente = Redis(connection_pool=pool)
    pila = AsyncExitStack()
    pila.push_async_callback(pool.disconnect)
    pila.push_async_callback(cliente.aclose)
    # from_conn_string crea los índices de RediSearch (asetup).
    async with pila, AsyncRedisSaver.from_conn_string(
        redis_client=cliente, ttl={"default_ttl": CHECKPOINT_TTL_MINUTOS, "refresh_on_read": True}
    ) as saver:
        # RedisSaver guarda el estado como JSON (RedisJSON) y solo revive las clases de esta lista:
        # sin ella, PedidoViaje / Aporte / ... vuelven del checkpoint como dict y el grafo se rompe al reanudar.
        saver.serde = JsonPlusRedisSerializer(allowed_json_modules=TIPOS_DEL_ESTADO,
                                              allowed_msgpack_modules=TIPOS_DEL_ESTADO)
        log.info("Checkpointer: AsyncRedisSaver (%s)", redis_url.split("@")[-1])
        yield saver


def mermaid_produccion() -> str:
    """El diagrama Mermaid del grafo de la API (con el nodo HITL), generado desde el código."""
    componentes = crear_componentes(modo_mock=True)
    return diagrama_mermaid(construir_grafo_api(componentes, checkpointer=None))
