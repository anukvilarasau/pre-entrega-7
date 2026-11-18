"""
scripts/carga_concurrente.py — Prueba de carga: N pedidos concurrentes contra la API (por defecto 5).

1. Manda los N POST /tasks en paralelo (httpx async).
2. Hace polling de GET /tasks/{id} de todos a la vez hasta que cada uno llega a DONE o FAILED.
3. Si un job se pausa en el nodo HITL (solo con --aprobar script), lo aprueba con POST /approve.
4. Imprime la tabla final (job_id, estado, duración, aprobaciones, tokens, costo) y la latencia p50/p95.
5. Con --langsmith, además lee de LangSmith el costo y la latencia de cada traza (metadata job_id).
6. Guarda todo en traces/carga/carga_<fecha>.json.

Uso:
    python scripts/carga_concurrente.py                          # 5 pedidos con auto_aprobar=true
    python scripts/carga_concurrente.py --aprobar script         # sin auto_aprobar: el script aprueba cada pausa
    python scripts/carga_concurrente.py --url http://localhost:8000 --langsmith
"""

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))

from app.observability import percentil  # noqa: E402

# Cinco viajes distintos (destinos distintos: ninguno sale de la memoria del otro).
PREGUNTAS: list[str] = [
    "Quiero ir 7 días a Bariloche en enero, somos 2 personas, salimos desde Mendoza y tenemos 800.000 pesos. "
    "¿Me armás el viaje y me decís si nos alcanza?",
    "Queremos ir 5 días a Salta en abril, somos 3 personas, salimos desde Córdoba y tenemos 1.200.000 pesos. "
    "¿Nos alcanza? Armanos un itinerario.",
    "Somos 2 y queremos ir 4 días a Mendoza en marzo desde Buenos Aires con 900.000 pesos. ¿Nos alcanza para "
    "bodegas y alta montaña?",
    "¿Qué clima hace en Ushuaia en agosto y qué actividades de nieve hay?",
    "Quiero ir 6 días a Puerto Madryn en octubre, somos 2 personas, salimos desde Buenos Aires y tenemos "
    "1.500 dólares. ¿Me armás el viaje y me decís si alcanza?",
]


def leer_argumentos() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Prueba de carga concurrente contra la API de la PE7.")
    p.add_argument("--url", default=os.getenv("API_URL", "http://localhost:8000"))
    p.add_argument("-n", type=int, default=5, help="Cantidad de pedidos concurrentes (por defecto 5).")
    p.add_argument("--aprobar", choices=["auto", "script"], default="auto",
                   help="auto: auto_aprobar=true en cada request. script: el script aprueba cada pausa HITL.")
    p.add_argument("--intervalo", type=float, default=2.0, help="Segundos entre consultas de estado.")
    p.add_argument("--timeout", type=float, default=1200.0, help="Tope total en segundos.")
    p.add_argument("--langsmith", action="store_true", help="Leer costo y latencia de cada traza en LangSmith.")
    return p.parse_args()


async def seguir(c: httpx.AsyncClient, job_id: str, args: argparse.Namespace, inicio: float) -> dict[str, Any]:
    """Polling de un job hasta DONE/FAILED; aprueba las pausas si --aprobar script."""
    aprobadas = 0
    while True:
        job = (await c.get(f"/tasks/{job_id}")).json()
        if job["estado"] in ("DONE", "FAILED"):
            job["_aprobadas_por_script"] = aprobadas
            job["_latencia_cliente_s"] = round(time.perf_counter() - inicio, 2)
            return job
        if job["estado"] == "WAITING_APPROVAL" and args.aprobar == "script":
            r = await c.post(f"/tasks/{job_id}/approve", json={"aprobado": True, "comentario": "prueba de carga"})
            aprobadas += r.status_code == 202
            print(f"  · {job_id[:8]} pausado en aprobacion_web → aprobado por el script")
        await asyncio.sleep(args.intervalo)


def resumen_aprobaciones(job: dict[str, Any]) -> str:
    registros = (job.get("resultado") or {}).get("aprobaciones_hitl") or []
    if not registros:
        return "-"
    return ", ".join(f"{r['resultado']} ({r['origen']}{', pausa' if r['pausa'] else ''})" for r in registros)


def datos_langsmith(jobs: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Costo y latencia de la traza raíz de cada job en LangSmith (puede tardar unos segundos en aparecer)."""
    import warnings

    from langsmith import Client

    warnings.filterwarnings("ignore", message=".*list_runs.*deprecated")  # langsmith 0.15: sigue funcionando hasta 2027
    cliente = Client()
    proyecto = os.getenv("LANGSMITH_PROJECT", "pre-entrega-7")
    salida: dict[str, dict[str, Any]] = {}
    for job in jobs:
        filtro = f'and(eq(metadata_key, "job_id"), eq(metadata_value, "{job["job_id"]}"))'
        runs = list(cliente.list_runs(project_name=proyecto, filter=filtro, is_root=True))
        costo = sum(float(r.total_cost or 0) for r in runs)
        latencias = [(r.end_time - r.start_time).total_seconds() for r in runs if r.end_time and r.start_time]
        salida[job["job_id"]] = {"trazas": len(runs), "costo_usd": round(costo, 6),
                                 "latencia_s": round(sum(latencias), 2),
                                 "tokens": sum(int(r.total_tokens or 0) for r in runs)}
    return salida


def guardar_markdown(jobs: list[dict[str, Any]], ls: dict[str, dict[str, Any]], salud: dict[str, Any],
                     args: argparse.Namespace, total: float, p50: float | None, p95: float | None,
                     costo_total: float, archivo: Path) -> Path:
    """La tabla de resultados en screenshots/resultados_carga.md (evidencia versionada junto a las capturas)."""
    filas = [
        f"# Prueba de carga — {len(jobs)} pedidos concurrentes ({datetime.now():%d/%m/%Y %H:%M})",
        "",
        f"API en modo **{salud['modo']}** · {salud['redis']['tipo']} · {salud['checkpointer']} · modelo "
        f"`{salud['modelo']}` · LangSmith proyecto `{salud['langsmith']['proyecto']}` · aprobación: `{args.aprobar}`",
        "",
        "| job_id | pregunta | estado | duración (s) | ejecución (s) | tokens | costo estimado | costo LangSmith | "
        "búsquedas web | aprobaciones HITL |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for j in jobs:
        m = (j.get("resultado") or {}).get("metricas") or {}
        costo_ls = f"${ls[j['job_id']]['costo_usd']:.4f}" if j["job_id"] in ls else "-"
        estado = j["estado"] + (f" ({j['error'][:60]})" if j.get("error") else "")
        filas.append(
            f"| `{j['job_id'][:8]}` | {j['pregunta'][:55]}… | {estado} | {j.get('duracion_s') or 0:.1f} | "
            f"{j.get('tiempo_ejecucion_s') or 0:.1f} | {j.get('total_tokens') or 0:,} | "
            f"${(j.get('costo_usd') or 0):.4f} | {costo_ls} | {m.get('busquedas_web', '-')} | "
            f"{resumen_aprobaciones(j)} |")
    costo_ls_total = f" (LangSmith: ${sum(v['costo_usd'] for v in ls.values()):.4f})" if ls else ""
    filas += [
        "",
        f"- **DONE:** {sum(j['estado'] == 'DONE' for j in jobs)}/{len(jobs)} · **tiempo total:** {total:.1f} s",
        f"- **Latencia por job** (de POST a DONE, incluye la espera de aprobación): p50 **{p50 or 0:.1f} s** · "
        f"p95 **{p95 or 0:.1f} s**",
        f"- **Costo total estimado:** ${costo_total:.4f}{costo_ls_total}",
        f"- Detalle completo (JSON): `{archivo.relative_to(RAIZ).as_posix()}`",
        "",
    ]
    salida = RAIZ / "screenshots" / "resultados_carga.md"
    salida.parent.mkdir(exist_ok=True)
    salida.write_text("\n".join(filas), encoding="utf-8")
    return salida


async def main() -> int:
    args = leer_argumentos()
    for flujo in (sys.stdout, sys.stderr):
        if hasattr(flujo, "reconfigure"):
            flujo.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
    preguntas = [PREGUNTAS[i % len(PREGUNTAS)] for i in range(args.n)]
    async with httpx.AsyncClient(base_url=args.url, timeout=30) as c:
        salud = (await c.get("/health")).json()
        print(f"API {args.url} · modo {salud['modo']} · {salud['redis']['tipo']} · {salud['checkpointer']} · "
              f"LangSmith {'ON' if salud['langsmith']['tracing'] else 'OFF'} ({salud['langsmith']['proyecto']})")
        inicio = time.perf_counter()
        creados = await asyncio.gather(*[
            c.post("/tasks", json={"pregunta": p, "auto_aprobar": args.aprobar == "auto"}) for p in preguntas
        ])
        ids = [r.json()["job_id"] for r in creados]
        print(f"{len(ids)} POST /tasks en {time.perf_counter() - inicio:.2f} s (todos devolvieron 202 + job_id)")
        jobs = await asyncio.wait_for(asyncio.gather(*[seguir(c, i, args, inicio) for i in ids]), args.timeout)
        total = time.perf_counter() - inicio

    ls: dict[str, dict[str, Any]] = {}
    if args.langsmith:
        await asyncio.sleep(10)  # LangSmith procesa las trazas en segundo plano
        ls = datos_langsmith(jobs)

    print()
    cab = f"{'job_id':<10} {'estado':<7} {'duración':>9} {'ejecución':>9} {'tokens':>8} {'costo est.':>10}"
    cab += f" {'costo LS':>9}" if ls else ""
    print(cab + "  aprobaciones")
    print("-" * (len(cab) + 40))
    for j in jobs:
        fila = (f"{j['job_id'][:8]:<10} {j['estado']:<7} {j.get('duracion_s') or 0:>8.1f}s "
                f"{j.get('tiempo_ejecucion_s') or 0:>8.1f}s {j.get('total_tokens') or 0:>8} "
                f"{'$' + format(j['costo_usd'], '.4f') if j.get('costo_usd') is not None else 'n/d':>10}")
        if ls:
            fila += f" {'$' + format(ls[j['job_id']]['costo_usd'], '.4f'):>9}"
        print(fila + f"  {resumen_aprobaciones(j)}" + (f"  ERROR: {j['error']}" if j.get("error") else ""))
    duraciones = [j["duracion_s"] for j in jobs if j.get("duracion_s") is not None]
    costos = [j["costo_usd"] for j in jobs if j.get("costo_usd") is not None]
    p50, p95 = percentil(duraciones, 50), percentil(duraciones, 95)
    print("-" * (len(cab) + 40))
    print(f"DONE: {sum(j['estado'] == 'DONE' for j in jobs)}/{len(jobs)} · tiempo total de la prueba: {total:.1f} s · "
          f"latencia p50: {p50 or 0:.1f} s · p95: {p95 or 0:.1f} s · costo estimado total: ${sum(costos):.4f}")

    carpeta = RAIZ / "traces" / "carga"
    carpeta.mkdir(parents=True, exist_ok=True)
    archivo = carpeta / f"carga_{salud['modo']}_{datetime.now():%Y%m%d-%H%M%S}.json"
    archivo.write_text(json.dumps({
        "url": args.url, "n": args.n, "aprobar": args.aprobar, "health": salud, "tiempo_total_s": round(total, 2),
        "latencia_p50_s": p50, "latencia_p95_s": p95, "costo_estimado_total_usd": round(sum(costos), 6),
        "langsmith": ls, "jobs": jobs,
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Detalle guardado en {archivo.relative_to(RAIZ)}")
    if salud["modo"] == "real":
        md = guardar_markdown(jobs, ls, salud, args, total, p50, p95, sum(costos), archivo)
        print(f"Tabla guardada en {md.relative_to(RAIZ)}")
    return 0 if all(j["estado"] == "DONE" for j in jobs) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
