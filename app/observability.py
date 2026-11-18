"""
app/observability.py — Observabilidad: LangSmith + métricas propias de cada job.

LangSmith (trazas):
- Se activa con LANGSMITH_TRACING=true + LANGSMITH_API_KEY (en el .env). LangChain
  y LangGraph mandan solos cada corrida: el run raíz "asesor_de_viajes" y, adentro,
  un run por nodo con su nombre (supervisor, aprobacion_web, investigador,
  analista, sintesis), el subagente ReAct, cada herramienta y cada llamada a Claude.
- `config_de_job()` le pone a todas esas corridas la metadata job_id (= thread_id,
  así LangSmith agrupa en la vista "Threads" el tramo antes y después de la pausa
  HITL), el modo y el tramo, más tags para filtrar en el dashboard.
- En modo mock no se traza (salvo que LANGSMITH_TRACING=true lo pida explícitamente).

Costo: LangSmith calcula el costo con su tabla de precios por modelo. Además
cada job guarda su propio costo estimado (tokens × PRECIOS_USD_POR_MTOK), así
GET /tasks/{id} y la prueba de carga lo muestran aunque el dashboard no esté a mano.
"""

import logging
import math
import os
from collections.abc import Iterable
from typing import Any

import config

log = logging.getLogger(__name__)

PROYECTO_POR_DEFECTO: str = "pre-entrega-7"

# USD por millón de tokens (precios de la API de Anthropic, noviembre de 2026; prompts de hasta 100K tokens).
# Lectura de caché = 0,1× la entrada; escritura de caché (5 min) = 1,25× la entrada.
PRECIOS_USD_POR_MTOK: dict[str, dict[str, float]] = {
    "claude-haiku-5-5": {"input": 0.10, "output": 0.50, "cache_read": 0.01, "cache_creation": 0.125},
}


def _verdadero(valor: str | None) -> bool:
    return (valor or "").strip().lower() in {"1", "true", "si", "sí", "yes"}


def configurar_langsmith(modo_mock: bool) -> dict[str, Any]:
    """Deja las variables de LangSmith listas y devuelve el estado (lo muestra GET /health).

    - Con clave y LANGSMITH_TRACING=true → tracing activo, proyecto LANGSMITH_PROJECT (pre-entrega-7).
    - En modo mock se apaga, salvo que LANGSMITH_TRACING=true venga explícito en el entorno.
    """
    os.environ.setdefault("LANGSMITH_PROJECT", PROYECTO_POR_DEFECTO)
    hay_clave = bool(os.getenv("LANGSMITH_API_KEY", "").strip())
    pedido = _verdadero(os.getenv("LANGSMITH_TRACING"))
    activo = hay_clave and pedido
    if modo_mock and not pedido:
        activo = False
    os.environ["LANGSMITH_TRACING"] = "true" if activo else "false"
    estado = {"tracing": activo, "proyecto": os.environ["LANGSMITH_PROJECT"], "clave": hay_clave}
    if pedido and not hay_clave:
        log.warning("LANGSMITH_TRACING=true pero falta LANGSMITH_API_KEY: no se van a mandar trazas")
    log.info("LangSmith: %s", estado)
    return estado


def config_de_job(
    job_id: str,
    *,
    modo_mock: bool,
    tramo: int,
    callbacks: list[Any],
    recursion_limit: int = config.RECURSION_LIMIT,
) -> dict[str, Any]:
    """El RunnableConfig de una ejecución del grafo para un job (inicio o reanudación)."""
    return {
        "configurable": {"thread_id": job_id},     # checkpoint de LangGraph = un hilo por job
        "recursion_limit": recursion_limit,
        "callbacks": callbacks,
        "run_name": "asesor_de_viajes",
        "tags": ["pre-entrega-7", "api", "mock" if modo_mock else "real", f"tramo-{tramo}"],
        "metadata": {
            "job_id": job_id,
            "thread_id": job_id,                   # LangSmith agrupa los tramos del job en "Threads"
            "modo": "mock" if modo_mock else "real",
            "tramo": tramo,                        # 1 = inicio, 2+ = reanudación después de la pausa HITL
            "modelo": config.ANTHROPIC_MODEL,
        },
    }


def sumar_tokens(acumulado: dict[str, dict[str, Any]], nuevo: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Suma el usage_metadata por modelo de dos tramos (UsageMetadataCallbackHandler)."""
    total = {m: dict(u) for m, u in acumulado.items()}
    for modelo, uso in (nuevo or {}).items():
        destino = total.setdefault(modelo, {})
        for clave, valor in dict(uso).items():
            if isinstance(valor, (int, float)):
                destino[clave] = destino.get(clave, 0) + valor
            elif isinstance(valor, dict):
                sub = destino.setdefault(clave, {})
                for k, v in valor.items():
                    if isinstance(v, (int, float)):
                        sub[k] = sub.get(k, 0) + v
    return total


def _precio(modelo: str) -> dict[str, float] | None:
    for nombre, precio in PRECIOS_USD_POR_MTOK.items():
        if modelo.startswith(nombre):
            return precio
    return None


def costo_usd(tokens: dict[str, dict[str, Any]]) -> float | None:
    """Costo estimado de un job a partir de sus tokens. None si algún modelo no tiene precio cargado."""
    total = 0.0
    for modelo, uso in tokens.items():
        precio = _precio(modelo)
        if precio is None:
            return None
        detalle_in = uso.get("input_token_details", {}) or {}
        cache_read = detalle_in.get("cache_read", 0) or 0
        cache_creation = detalle_in.get("cache_creation", 0) or 0
        entrada_normal = max(0, (uso.get("input_tokens", 0) or 0) - cache_read - cache_creation)
        total += (entrada_normal * precio["input"] + cache_read * precio["cache_read"]
                  + cache_creation * precio["cache_creation"]
                  + (uso.get("output_tokens", 0) or 0) * precio["output"]) / 1_000_000
    return round(total, 6)


def total_tokens(tokens: dict[str, dict[str, Any]]) -> int:
    return int(sum((u.get("total_tokens", 0) or 0) for u in tokens.values()))


def percentil(valores: Iterable[float], p: float) -> float | None:
    """Percentil p (0-100) con interpolación lineal (el mismo criterio que numpy por defecto)."""
    datos = sorted(valores)
    if not datos:
        return None
    if len(datos) == 1:
        return datos[0]
    k = (len(datos) - 1) * p / 100
    piso, techo = math.floor(k), math.ceil(k)
    return datos[piso] + (datos[techo] - datos[piso]) * (k - piso)


async def esperar_envio() -> None:
    """Antes de apagar la API: que salgan las trazas que quedaron en la cola de LangSmith."""
    if os.getenv("LANGSMITH_TRACING") != "true":
        return
    import asyncio

    from langchain_core.tracers.langchain import wait_for_all_tracers

    await asyncio.to_thread(wait_for_all_tracers)
