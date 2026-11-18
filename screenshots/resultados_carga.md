# Prueba de carga — 5 pedidos concurrentes (18/11/2026 14:40)

API en modo **real** · redis · AsyncRedisSaver · modelo `claude-haiku-5-5` · LangSmith proyecto `pre-entrega-7` · aprobación: `script`

| job_id | pregunta | estado | duración (s) | ejecución (s) | tokens | costo estimado | costo LangSmith | búsquedas web | aprobaciones HITL |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| `17d521cf` | Quiero ir 7 días a Bariloche en enero, somos 2 personas… | DONE | 79.3 | 73.1 | 111,905 | $0.0164 | $0.0164 | 1 | aprobada (persona, pausa) |
| `abf031f5` | Queremos ir 5 días a Salta en abril, somos 3 personas, … | DONE | 85.7 | 82.6 | 105,389 | $0.0165 | $0.0165 | 0 | sin_pedir (memoria), sin_pedir (memoria) |
| `9c06a718` | Somos 2 y queremos ir 4 días a Mendoza en marzo desde B… | DONE | 92.3 | 89.2 | 143,798 | $0.0214 | $0.0214 | 0 | sin_pedir (memoria), sin_pedir (memoria) |
| `1ec8b191` | ¿Qué clima hace en Ushuaia en agosto y qué actividades … | DONE | 35.3 | 32.4 | 29,096 | $0.0048 | $0.0048 | 0 | sin_pedir (memoria) |
| `f996b969` | Quiero ir 6 días a Puerto Madryn en octubre, somos 2 pe… | DONE | 70.4 | 62.6 | 71,031 | $0.0117 | $0.0117 | 0 | aprobada (persona, pausa) |

- **DONE:** 5/5 · **tiempo total:** 92.8 s
- **Latencia por job** (de POST a DONE, incluye la espera de aprobación): p50 **79.3 s** · p95 **91.0 s**
- **Costo total estimado:** $0.0708 (LangSmith: $0.0708)
- Detalle completo (JSON): `traces/carga/carga_real_20261118-144057.json`
