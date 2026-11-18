"""Tests del nodo human-in-the-loop (app/hitl.py) sobre el grafo real en modo mock, con checkpointer."""

from datetime import date
from pathlib import Path
from typing import Any

import pytest
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command

import config
from agents.research_tools import PERMISO_WEB, crear_herramientas_investigador
from agents.synthesizer import AVISO_SIN_WEB
from app.graph import construir_grafo_api
from app.hitl import interpretar_respuesta
from graph import Componentes, crear_componentes, diagrama_mermaid
from state import estado_inicial

DEMO = ("Quiero ir 7 días a Bariloche en enero, somos 2 personas, salimos desde Mendoza y tenemos "
        "800.000 pesos. ¿Me armás el viaje y me decís si nos alcanza?")


@pytest.fixture(autouse=True)
def hoy_fijo(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(config, "FECHA_REFERENCIA", date(2026, 11, 5))
    monkeypatch.setattr(config, "TRACES_DIR", tmp_path / "traces")


@pytest.fixture
def comp(tmp_path: Path) -> Componentes:
    return crear_componentes(modo_mock=True, ruta_memoria=tmp_path / "chroma")


def cfg(thread: str) -> dict[str, Any]:
    return {"configurable": {"thread_id": thread}, "recursion_limit": config.RECURSION_LIMIT}


async def hasta_la_pausa(grafo: Any, thread: str, auto_aprobar: bool = False) -> Any:
    await grafo.ainvoke(estado_inicial(DEMO, auto_aprobar=auto_aprobar), cfg(thread))
    return await grafo.aget_state(cfg(thread))


def test_el_grafo_de_la_api_intercala_el_nodo_hitl(comp: Componentes) -> None:
    g = construir_grafo_api(comp, checkpointer=None).get_graph()
    assert "aprobacion_web" in g.nodes
    aristas = {(a.source, a.target, a.conditional) for a in g.edges}
    assert ("supervisor", "aprobacion_web", True) in aristas
    assert ("aprobacion_web", "investigador", False) in aristas
    assert ("supervisor", "investigador", True) not in aristas  # ya no se llega directo a la web
    assert "aprobacion_web --> investigador" in diagrama_mermaid(construir_grafo_api(comp, checkpointer=None))


async def test_pausa_antes_de_la_web_y_muestra_que_quiere_buscar(comp: Componentes) -> None:
    grafo = construir_grafo_api(comp, InMemorySaver())
    snap = await hasta_la_pausa(grafo, "t-pausa")
    assert snap.next == ("aprobacion_web",)
    pedido = snap.interrupts[0].value
    assert pedido["tipo"] == "aprobacion_busqueda_web" and pedido["destino"] == "Bariloche"
    assert {"pasajes", "alojamiento"} <= set(pedido["temas_sin_memoria"]) and pedido["temas_en_memoria"] == []
    assert any("Mendoza → Bariloche" in c for c in pedido["consultas_previstas"])
    assert comp.buscador.llamadas == []  # type: ignore[attr-defined] — ninguna búsqueda web antes del sí
    assert not snap.values.get("aportes")


async def test_reanudar_aprobado_busca_en_la_web_y_termina(comp: Componentes) -> None:
    grafo = construir_grafo_api(comp, InMemorySaver())
    await hasta_la_pausa(grafo, "t-si")
    final = await grafo.ainvoke(Command(resume={"aprobado": True, "comentario": "dale"}), cfg("t-si"))
    assert final["permiso_web"] == "aprobada" and final["respuesta_final"]
    assert comp.buscador.llamadas  # type: ignore[attr-defined]
    registro = final["aprobaciones"][0]
    assert registro.pausa and registro.origen == "persona" and registro.comentario == "dale"
    assert any(f.origen == "web" for a in final["aportes"] for f in a.fuentes)
    assert AVISO_SIN_WEB not in final["respuesta_final"]


async def test_reanudar_rechazado_sigue_sin_web_y_la_sintesis_lo_aclara(comp: Componentes) -> None:
    grafo = construir_grafo_api(comp, InMemorySaver())
    await hasta_la_pausa(grafo, "t-no")
    final = await grafo.ainvoke(Command(resume={"aprobado": False}), cfg("t-no"))
    assert final["permiso_web"] == "rechazada"
    assert comp.buscador.llamadas == []  # type: ignore[attr-defined] — Tavily nunca se llamó
    assert final["respuesta_final"].startswith(AVISO_SIN_WEB)
    llamadas_web = [ll for a in final["aportes"] for ll in a.llamadas if ll.herramienta == "buscar_en_web"]
    assert llamadas_web and all(ll.resultado.startswith(("WEB NO AUTORIZADA", "NO SE BUSCÓ")) for ll in llamadas_web)
    # un solo pedido de permiso por job: si el supervisor vuelve al investigador, se respeta el "no"
    assert all(r.resultado == "rechazada" for r in final["aprobaciones"])


async def test_si_la_memoria_responde_no_hay_pausa(comp: Componentes) -> None:
    grafo = construir_grafo_api(comp, InMemorySaver())
    await grafo.ainvoke(estado_inicial(DEMO, auto_aprobar=True), cfg("t-llena"))  # llena la memoria
    web_antes = len(comp.buscador.llamadas)  # type: ignore[attr-defined]

    final = await grafo.ainvoke(estado_inicial(DEMO), cfg("t-memoria"))  # misma pregunta, otro job
    assert not (await grafo.aget_state(cfg("t-memoria"))).interrupts
    assert final["aprobaciones"][0].origen == "memoria" and not final["aprobaciones"][0].pausa
    assert final["permiso_web"] == "sin_pedir" and final["respuesta_final"]
    assert len(comp.buscador.llamadas) == web_antes  # type: ignore[attr-defined]


async def test_auto_aprobar_no_pausa(comp: Componentes) -> None:
    grafo = construir_grafo_api(comp, InMemorySaver())
    final = await grafo.ainvoke(estado_inicial(DEMO, auto_aprobar=True), cfg("t-auto"))
    assert final["aprobaciones"][0].origen == "auto_aprobar" and final["permiso_web"] == "aprobada"
    assert comp.buscador.llamadas  # type: ignore[attr-defined]


def test_cualquier_respuesta_rara_cuenta_como_rechazo() -> None:
    assert interpretar_respuesta(True).aprobado
    assert interpretar_respuesta({"aprobado": True}).aprobado
    for raro in ("sí", None, {"ok": 1}, 1):
        assert not interpretar_respuesta(raro).aprobado


async def test_la_herramienta_web_no_llama_a_tavily_sin_permiso(comp: Componentes) -> None:
    _, web = crear_herramientas_investigador(comp.memoria, comp.buscador)
    args = {"consulta": "precio pasajes Mendoza Bariloche", "destino": "Bariloche", "tema": "pasajes"}
    for permiso in ("rechazada", "sin_pedir"):
        token = PERMISO_WEB.set(permiso)  # type: ignore[arg-type]
        try:
            texto = await web.ainvoke(args)
        finally:
            PERMISO_WEB.reset(token)
        assert texto.startswith("WEB NO AUTORIZADA")
    assert comp.buscador.llamadas == []  # type: ignore[attr-defined]
    assert "WEB (Tavily)" in await web.ainvoke(args)  # sin HITL ("libre") se comporta como en la PE6


def test_hay_vigentes_mira_destino_tema_y_antiguedad(comp: Componentes) -> None:
    from datetime import datetime, timedelta

    from agents.memory import ResultadoWeb

    m = comp.memoria
    assert not m.hay_vigentes("Bariloche", "pasajes")
    vieja = datetime.now().astimezone() - timedelta(days=config.MEMORIA_MAX_DIAS + 1)
    m.guardar([ResultadoWeb("https://ej.com/a", "Pasajes", "$100")], "Bariloche", "pasajes", "pasajes", vieja)
    assert not m.hay_vigentes("Bariloche", "pasajes")  # vencido
    m.guardar([ResultadoWeb("https://ej.com/b", "Pasajes", "$100")], "Bariloche", "pasajes", "pasajes")
    assert m.hay_vigentes("bariloché", "pasajes") and not m.hay_vigentes("Bariloche", "clima")
    # la cotización del dólar vale para cualquier destino (lo mostró la prueba de carga real)
    m.guardar([ResultadoWeb("https://ej.com/dolar", "Dólar hoy", "US$ 1 = $1.450")], "Buenos Aires", "cambio", "dólar")
    assert m.hay_vigentes("Puerto Madryn", "cambio")
    assert m.buscar("cotización dólar", "Puerto Madryn", "cambio").hay_utiles
