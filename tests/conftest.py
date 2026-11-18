"""Fixtures compartidas: todo corre offline, sin claves y en carpetas temporales."""

import os
from pathlib import Path

import pytest

from agents.memory import EmbeddingHash, MemoriaBusquedas
from agents.research_tools import BuscadorMock


@pytest.fixture(autouse=True)
def sin_langsmith(monkeypatch: pytest.MonkeyPatch) -> None:
    """Los tests nunca mandan trazas a LangSmith, aunque el .env tenga LANGSMITH_TRACING=true."""
    monkeypatch.setenv("LANGSMITH_TRACING", "false")
    monkeypatch.delenv("LANGCHAIN_TRACING_V2", raising=False)
    assert os.environ["LANGSMITH_TRACING"] == "false"


@pytest.fixture
def memoria(tmp_path: Path) -> MemoriaBusquedas:
    """ChromaDB nueva en una carpeta temporal, con el embedding offline."""
    return MemoriaBusquedas(ruta=tmp_path / "chroma", embedding=EmbeddingHash())


@pytest.fixture
def buscador() -> BuscadorMock:
    """Tavily simulado (lee data/mock_busquedas.json)."""
    return BuscadorMock()
