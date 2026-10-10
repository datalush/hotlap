# Aceptación de providers nativos de DataFusion

Los identificadores históricos de verificación y la proveniencia de providers
se registran a continuación.

El motor probado fue `SessionContext`/`SessionState` y `RuntimeEnv` de DataFusion
de este repositorio, ejecutando los providers Rust reales de Fluss. Estas pruebas
no requirieron aplicación externa ni nueva implementación de planificador/checkpoints.
El runtime de Hotlap es otra capa de integración; estos resultados no lo certifican.

> Estos resultados corresponden solo a providers nativos de DataFusion y sesiones
> del llamador; no son evidencia de aceptación del runtime Hotlap.

## Stack y límite de reproducibilidad

Evidencia del árbol de trabajo del 2026-10-05, basado en
`adc4f70f760904e795a9cd81a7ce728581286f55` (cambios sin commit): DataFusion
55.1.0, Arrow 59.3.0 resuelto, cliente Fluss Rust 1.0.0 y Fluss Docker
`ghcr.io/midnattsol/fluss:1.0.0-midnattsol.6`. El backport del planner nativo
DataFusion y el checksum del archivo original
están documentados en [proveniencia vendor](../vendor/README.md). Los Cargo.lock
raíz y del cliente fijan los grafos nativos activos.

La cobertura de sesión del llamador/operadores/ejemplos está versionada en commits
`2a85deb`, `043a346` y `1a4556b`, posteriores al retiro, endurecimiento de fallos y trabajo
del motor. Los harnesses de perfiles e instrumentación ScanKv están en `19dca49`.
Las ejecuciones del árbol registradas abajo preceden esos commits; la verificación
Git limpia se informa aparte.

Hace falta reproducir desde checkout limpio con los cambios fuente y el retiro ya
confirmados. Ejecutar este árbol no demuestra que el HEAD actual por sí solo
reproduzca las nuevas pruebas.

## Cobertura a nivel de motor

| Contrato | Ejecución/assertion real del motor |
| --- | --- |
| Sesión/planner/UDF/runtime llamador | `write_sql::insert_log_and_upsert_kv_from_sql` combina `RecordingPlanner`, UDF `is_two` registrada, cuatro particiones destino y `GreedyMemoryPool` acotado de 16 MiB del llamador. Comprueba identidad del pool. Grafos auxiliares DELETE/MERGE pasan por planner llamador y usan su UDF. |
| Distribución sink nativo | Esa prueba suministra tres particiones entrada; optimizador estándar impone requisito sink single-partition. Ejecutar dos veces mismo INSERT físico almacena dos copias, con comprobaciones explícitas de datos finales. |
| Operadores y semántica NULL | `live_log_sql::empty_log_and_new_offsets_on_each_query` compara siete SELECT Fluss reales con el mismo motor leyendo referencia Arrow: proyecciones, comparaciones residuales, filtros NULL, COUNT/SUM agrupados, joins inner/left y ORDER BY NULLS FIRST/LIMIT. |
| Política de recursos host | Prueba referencia usa dos particiones destino, lote 64, pool 16 MiB y reservas spill sort nativo de 1 MiB/partición. Reservas vuelven a cero al liberar resultados, operadores, reejecuciones y fallos. |
| Catálogo/metadatos/proyección/métricas | `bounded_log_and_kv_sql_against_native_sni`, `partitioned_logs_and_kv_discover_each_execution` y helpers prueban registro catálogo, esquema/proyección fuente, conteos efectivos bucket, paralelismo físico y métricas EXPLAIN ANALYZE. |
| Ciclo de vida finito/continuo | Suite live ejercita starts vacíos, offsets fin finitos nuevos por ejecución, tres ejecuciones solapadas de un plan fuente, consumidores lentos reteniendo, appends tardíos, espera idle y cancelación. |
| Identidad snapshot/esquema/topología | Suite live comprueba aislamiento/evolución snapshot KV, rechazo de tabla recreada/modificada e invalidación topología streaming, sin cambio silencioso de identidad. |
| DML nativo y efectos parciales | Suites SQL escritura, autorización y pressure ejercitan INSERT/DELETE/MERGE, orden NULL/acciones, routing, aislamiento peers, cancelación, incertidumbre ACK y datos finales. Ver [fallos nativos](native-failure-verification.md). |
| Fallo y recuperación | Suite live ejercita failover coordinador aislado y recuperación explícita con consulta nueva. Rechazo memoria, invalidación tabla y pérdida socket/reinicio usan errores/limpieza nativos; evidencia remota/STS está enlazada desde preparación para producción. |

### Límite descubierto en la política de ordenamiento del host

La primera ejecución ampliada de suite live pasó ocho pruebas, pero la prueba de
operador referencia falló con `ResourcesExhausted`: dos reservas sort spill default
de 10 MiB no caben en pool 16 MiB. `SessionConfig` host configura ahora
`execution.sort_spill_reservation_bytes = 1 MiB` para estas filas de referencia pequeñas.
El provider no modifica política runtime. Es rechazo real de pool cooperativo, no
fallo decoder fuente ni prueba de límite RSS proceso. Prueba dirigida pasó después,
seguida de las nueve pruebas live.

## Comandos y resultados verificados

Builds funcionales: DEBUG, ocho jobs compilación. Prueba DML sesión modificada pasó
en **10,63s**; suite engine live completa pasó **9/9 en 81,55s**. Verificación de
fallos/permisos registra aparte ocho pruebas SQL escritura, presión/recuperación,
autorización y regresiones core/cliente/planner nativas.

```sh
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 uv run --no-project --env-file ../lab/.env cargo test -p fluss-datafusion --locked --test write_sql insert_log_and_upsert_kv_from_sql -- --ignored --test-threads=1
KUBECONFIG=/tmp/opencode/native-sni.kubeconfig CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 uv run --no-project --env-file ../lab/.env cargo test -p fluss-datafusion --locked --test live_log_sql -- --ignored --test-threads=1
```

Prueba failover reinicia solo coordinador activo en namespace native-sni aislado.
Fixtures tabla usan nombres únicos y eliminan sus tablas. Credenciales de laboratorio
permanecen en archivo entorno ignorado.

También pasó [perfil corto lectura RELEASE de ruta final](native-profile-plan.md):
cuatro SELECT log/KV concurrentes sobre dataset log mayor que pool, consumidores
lentos, olas repetidas/cancelación y limpieza recursos explícita. Midió 128 scans en
120,3 s, VmHWM proceso 118 MiB y pico reserva 32 MiB. Añade evidencia medida runtime,
no sustituye perfiles sostenidos/ruta escritura.

Perfiles ruta final registrados pasaron: matriz writer continuo cuatro casos 60s/
300s y reader 5+30 min, con comprobaciones latencia/recursos/datos/limpieza y controles
directos de asignación. Ejemplo final fuente aislada ejecuta también SQL log/KV nativo
con política host/runtime separada; ver [evidencia de fuente](native-cleanup-audit.md).

Ejecución Git limpia de la serie versionada está [registrada](native-checkout-verification.md),
incluido smoke remoto RustFS aprobado tras recuperar endpoint. Aceptación funcional
nativa, perfiles y fuente limpia tiene evidencia verificada; garantías de perfil se
limitan a setup registrado. ACK/offsets ofrecidos son observaciones, no checkpoints
durables del motor ni recuperación exactly-once.
