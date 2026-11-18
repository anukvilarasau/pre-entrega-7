"""
graph.py — El grafo principal: topología JERÁRQUICA con un supervisor al centro.

            ┌──────────────┐
  START ──► │  supervisor  │ ──(FINALIZAR)──► sintesis ──► END
            └──────────────┘
              ▲   │     │
              │   ▼     ▼
        investigador   analista      (los dos vuelven SIEMPRE al supervisor)

- `add_conditional_edges("supervisor", ruta_supervisor, ...)`: después de cada
  decisión, `ruta_supervisor` (que devuelve un Literal con el nombre del
  nodo) elige la arista. Los especialistas no hablan entre ellos.
- Los especialistas son subgrafos ReAct (`create_react_agent`) envueltos en
  un nodo que acota su contexto y valida lo que escriben en el estado.
- Frenos: MAX_PASOS_SUPERVISOR (supervisor), RECURSION_LIMIT (este grafo) y
  RECURSION_LIMIT_ESPECIALISTA (ciclo interno de cada especialista).

`consultar()` corre UNA pregunta de punta a punta, registra cada paso en una
traza y la guarda en traces/. La usan main.py, demo.ipynb y los tests.
"""

import logging
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from langchain_core.callbacks import UsageMetadataCallbackHandler
from langchain_core.language_models import BaseChatModel
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.errors import GraphRecursionError
from langgraph.graph import END, START, StateGraph
from langgraph.graph.state import CompiledStateGraph

import config
from agents.analyst_agent import crear_agente_analista, crear_nodo_analista
from agents.analyst_tools import crear_herramientas_analista
from agents.common import NodoAsync
from agents.memory import EmbeddingHash, MemoriaBusquedas
from agents.research_agent import crear_agente_investigador, crear_nodo_investigador
from agents.research_tools import BuscadorMock, BuscadorTavily, BuscadorWeb, crear_herramientas_investigador
from agents.supervisor import crear_nodo_supervisor, ruta_supervisor
from agents.synthesizer import crear_nodo_sintesis
from llm import Rol, crear_llm
from state import OrquestadorState, estado_inicial
from tracing import Traza

log = logging.getLogger(__name__)

ROLES: tuple[Rol, ...] = ("supervisor", "investigador", "analista", "sintesis")


@dataclass
class Componentes:
    """Las piezas "de afuera" del grafo: cambian entre modo real y mock; el grafo no."""

    memoria: MemoriaBusquedas
    buscador: BuscadorWeb
    llms: dict[str, BaseChatModel]
    modo_mock: bool


def crear_componentes(modo_mock: bool = config.MODO_MOCK, fallar_web: bool = False,
                      ruta_memoria: Path | None = None) -> Componentes:
    """Modo real: Claude + Tavily + ChromaDB en data/memoria_chroma/.
    Modo mock: LLM simulado + Tavily simulado + ChromaDB en una carpeta temporal."""
    if modo_mock:
        ruta = ruta_memoria or Path(tempfile.mkdtemp(prefix="memoria_mock_")) / "chroma"
        memoria = MemoriaBusquedas(ruta=ruta, embedding=EmbeddingHash())
        buscador: BuscadorWeb = BuscadorMock(fallar=fallar_web)
    else:
        memoria = MemoriaBusquedas(ruta=ruta_memoria or config.CHROMA_DIR)
        buscador = BuscadorTavily()
    llms = {rol: crear_llm(rol, modo_mock=modo_mock) for rol in ROLES}
    return Componentes(memoria=memoria, buscador=buscador, llms=llms, modo_mock=modo_mock)


def construir_grafo(
    componentes: Componentes,
    max_pasos: int | None = None,
    nodo_aprobacion: NodoAsync | None = None,
    checkpointer: BaseCheckpointSaver | None = None,
) -> CompiledStateGraph:
    """Arma y compila el StateGraph jerárquico.

    PE7: con `nodo_aprobacion` (app/hitl.py) se intercala el nodo human-in-the-loop
    "aprobacion_web" en el camino supervisor → investigador, y con `checkpointer`
    (AsyncRedisSaver en la API) cada paso queda guardado y el grafo se puede pausar
    y reanudar. Sin esos dos argumentos el grafo es idéntico al de la PE6.
    """
    investigador = crear_agente_investigador(
        componentes.llms["investigador"], crear_herramientas_investigador(componentes.memoria, componentes.buscador)
    )
    analista = crear_agente_analista(componentes.llms["analista"], crear_herramientas_analista())

    grafo = StateGraph(OrquestadorState)
    grafo.add_node("supervisor", crear_nodo_supervisor(componentes.llms["supervisor"], max_pasos=max_pasos))
    grafo.add_node("investigador", crear_nodo_investigador(investigador))
    grafo.add_node("analista", crear_nodo_analista(analista))
    if nodo_aprobacion is not None:
        grafo.add_node("aprobacion_web", nodo_aprobacion)
    grafo.add_node("sintesis", crear_nodo_sintesis(componentes.llms["sintesis"]))

    grafo.add_edge(START, "supervisor")
    # El supervisor decide; ruta_supervisor traduce la decisión al nodo siguiente.
    grafo.add_conditional_edges(
        "supervisor",
        ruta_supervisor,
        {"investigador": "aprobacion_web" if nodo_aprobacion is not None else "investigador",
         "analista": "analista", "sintesis": "sintesis"},
    )
    if nodo_aprobacion is not None:
        grafo.add_edge("aprobacion_web", "investigador")  # aprobada o rechazada, siempre sigue el investigador
    grafo.add_edge("investigador", "supervisor")  # los especialistas siempre devuelven el control
    grafo.add_edge("analista", "supervisor")
    grafo.add_edge("sintesis", END)
    return grafo.compile(name="asesor_de_viajes", checkpointer=checkpointer)


def diagrama_mermaid(grafo: CompiledStateGraph) -> str:
    """El diagrama del grafo en Mermaid (para el README)."""
    return grafo.get_graph().draw_mermaid()


async def consultar(
    grafo: CompiledStateGraph,
    pregunta: str,
    escenario: str = "",
    modo_mock: bool = config.MODO_MOCK,
    max_pasos: int | None = None,
    recursion_limit: int = config.RECURSION_LIMIT,
    guardar: bool = True,
) -> Traza:
    """Corre una pregunta de punta a punta y devuelve su traza (ya guardada en traces/).

    astream con dos modos a la vez:
    - "updates": lo que escribe cada nodo al terminar → la traza paso a paso;
    - "values": el estado completo después de cada paso → se conserva el último
      por si la corrida se corta por RECURSION_LIMIT.
    """
    traza = Traza(pregunta=pregunta, escenario=escenario, modo="mock" if modo_mock else "real",
                  max_pasos=max_pasos or config.MAX_PASOS_SUPERVISOR, recursion_limit=recursion_limit)
    tokens = UsageMetadataCallbackHandler()
    cfg: dict[str, Any] = {"recursion_limit": recursion_limit, "callbacks": [tokens]}
    ultimo: dict[str, Any] = {}
    try:
        async for modo, dato in grafo.astream(estado_inicial(pregunta), cfg, stream_mode=["updates", "values"]):
            if modo == "updates":
                for nodo, salida in dato.items():
                    traza.registrar(nodo, salida or {})
            else:
                ultimo = dato
        traza.cerrar("completo", ultimo, tokens.usage_metadata)
    except GraphRecursionError:
        aviso = (f"Me detuve de forma controlada: el grafo llegó a su límite de {recursion_limit} pasos "
                 "(RECURSION_LIMIT) antes de terminar.")
        traza.cerrar("cortado_por_recursion_limit", ultimo, tokens.usage_metadata, aviso=aviso)
    except Exception as e:  # API caída, clave inválida, etc.
        log.error("La corrida falló: %s: %s", type(e).__name__, e)
        traza.cerrar("error", ultimo, tokens.usage_metadata, aviso=f"{type(e).__name__}: {e}")
        if guardar:
            traza.guardar()
        raise
    if guardar:
        traza.guardar()
    return traza
