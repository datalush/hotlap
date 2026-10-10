# Auditoría de la implementación Rust

Instantánea histórica: las rutas FFI/Python y referencias de planes previos del
inventario original se conservan como historia, no como instrucciones activas.
Esas integraciones se retiraron; el workspace activo es solo Rust. La posterior
[migración Hotlap](hotlap-layout.md) también retiró árbol Java copiado y herramientas.

Fecha: **2026-10-04**. Alcance: primera auditoría arquitectónica del árbol de trabajo
de entonces; no es aceptación de producción ni refactorización. Bases: `d06288a`
(MERGE), `f66e90c` (INSERT/DELETE), `89e111b` (batch/streaming), con cambios FFI sin
commit encima. No modificó implementación, añadió bindings, inyectó fallos en vivo ni
ejecutó benchmarks. La evidencia de pruebas siguiente es histórica. Rutas/rangos
describen aquel árbol; API upstream se refiere a DataFusion 55.1.0 resuelto entonces.

## Límites de responsabilidad

- Cliente Rust: protocolo/autenticación/metadatos, routing tabla/partición/bucket,
  lectura, decode esquema, encoding, colas escritura, ACK y reintentos cliente.
- Conector Rust: contratos tabla DataFusion, adaptadores ejecución, pushdown
  conservador, capacidades, progreso ejecución, recursos y escrituras Fluss admitidas.
- DataFusion: SQL, evaluación exacta, planificación/optimización, operadores,
  distribución y su pool.
- Bindings exponen contratos, no dictan planificación Rust ni reimplementan SQL,
  routing, reintentos o gestor memoria.
- Motor gestiona checkpoints procesados, trabajos persistentes, conciliación y
  conflictos de negocio. Offsets ofrecidos/ACK no son commits del motor.

## Inventario del conector (15 módulos en la instantánea)

Rutas bajo `crates/fluss-datafusion/src/`.

| Módulo | Responsabilidad | Disposición histórica |
| --- | --- | --- |
| `lib.rs` | Exports públicos | Mantener delgado; introducción que solo mencionaba fuentes finitas quedó obsoleta con streaming/escrituras |
| `catalog.rs` | Adaptador catálogo snapshot | Opcional; política/plazos descubrimiento no son catálogo del motor |
| `log_table.rs` | Provider log, esquema, pushdown, paralelismo | Mantener; planificación nativa y filtros exactos fuera de la fuente |
| `kv_table.rs` | Provider snapshot y DML | Mantener; revisar planner default directo en DELETE |
| `log_options.rs` | Modo/posición inicial/plazos lectura | Separar transporte cliente; precisar alcance plazo |
| `log_progress.rs` | Progreso de lotes ofrecidos | Extender contrato, no convertir en almacén checkpoints |
| `scan.rs` | Streams log, validación/progreso ejecución | Mantener adaptador; contabilizar retención/ciclo de vida |
| `kv_scan.rs` | Streams snapshot bucket/métricas páginas | Evaluar decode/proyección y alcance reservas |
| `execution.rs` | Presentación plan/métricas sobre fuente hoja | Mantener salvo observabilidad nativa equivalente; no oculta grafo FFI |
| `filter.rs` | Traducción conservadora expresión DF → poda Fluss | Mantener; evaluación residual SQL exacta necesaria |
| `partitions.rs` | Descubrimiento/poda/presupuestos ejecución | Mantener; routing/protocolo siguen en cliente |
| `offsets.rs` | Capturas compartidas por contexto/generación | Mantener propósito; documentar/probar supuestos |
| `metrics.rs` | Métricas/guards de vida streams | Distinguir pico lote de memoria total retenida |
| `merge.rs` | Semántica MERGE sobre operadores DF | Mantener composición; revisar bypass planner/adaptador fuente |
| `write.rs` | Sink, validación destino, ejecución/resultados | Separar límites coherentes; cliente posee routing/encoding |

Inventario cliente y flujos log/KV se conservan en
[flujos de datos y recursos](rust-implementation-flows.md).

## Hallazgos técnicos históricos

La auditoría original organizó hallazgos y código transitorio con etiquetas
internas. Para no perder contenido técnico ni conservar referencias de
planificación, sus observaciones, clasificaciones, decisiones y evidencia se
reescribieron como temas técnicos en
[hallazgos](rust-implementation-findings.md). La observación de planificación DF
55.1 y el registro de código transitorio permanecen allí con referencias de fuente.

## Mapa de evidencia y comprobaciones pendientes entonces

| Evidencia del snapshot histórico | Alcance | Comprobaciones pendientes en ese momento |
| --- | --- | --- |
| `tests/live_log_sql.rs` | Proyección, pruning vs filtros exactos, evolución esquema, snapshots, reejecución/concurrencia y tablas recreadas | Planner/ruta nativa propia; lotes retenidos; proyección tras limpieza planner |
| `tests/remote_retention.rs`; `7306be7` | Perfiles RAM/disco/concurrencia y pressure remotos, S3/STS, retención y renovación real | Colas poll decodificadas, admisión pre-decode, lotes retenidos/ruta batch cambiada; repetir solo rutas modificadas |
| `tests/live_log_sql.rs`; `89e111b` | Appends tardíos/idle/cancelación, fallos topología y fuentes streaming | Progreso inicial vacío/podado, solapamiento mismo contexto, colas Arrow pendientes |
| `tests/write_sql.rs`; `f66e90c`, `d06288a` | INSERT log/KV, DELETE, MERGE, input 4 MiB/writer 2 MiB, concurrencia y layouts | Batch append mixto, cancelación ACK/enqueue, plazo preparación, resultado parcial |
| Cliente `record/arrow.rs`, `accumulator.rs`, scanners | Normalización esquema/NULL, limiter, límites reader/scanner | Identidad buffers prebuilt, layout efectivo, bytes retenidos vs permisos |
| Pruebas FFI históricas | Casos cross-library de buffers/vida y rechazo pool | No prueban decode/routing nativo ni paridad runtime completa |

Muchas integraciones se ignoraban por defecto y requerían fixtures Fluss o
Docker/RustFS aislados. Leerlas no equivale a ejecución. Casos nuevos debían probar
contratos integración, no duplicar operadores SQL DataFusion ni imponer estructura
de refactor.

## Resoluciones posteriores

Estas disposiciones pertenecen a snapshots posteriores; no reemplazan fechas,
hashes ni límites de la auditoría inicial. El detalle técnico e histórico completo
se conserva en [historial de resoluciones](rust-implementation-history-details.md).

- Planificación: DELETE/MERGE usan `create_physical_plan` de sesión suministrada;
  planner de registro prueba grafos internos. Se retiró coalesce manual y se probó
  enforcement nativo con MemTable multipartición. `InputPlan` se mantuvo como
  adaptador de fuente física MERGE. DataFusion 55.1 rechaza alias destino DELETE
  antes de planificación provider; no se añadió reescritura SQL.
- Recursos/lecturas: leases de buffers Arrow retienen reservas con consumidores,
  clones/slices; polling limitado redujo cola pendiente. Sin afirmación RSS estricta,
  protección pre-descompresión ni límite para downstream arbitrario.
- Progreso: `LogProgress` observa rangos iniciales/vacíos, lotes ofrecidos,
  exclusiones y terminalidad; no es checkpoint/retry del motor. Solapamiento con
  mismo `TaskContext` sigue como limitación registrada.
- Escritura: ruta Arrow usa metadata/layout efectivo y slices/gather; KV mantiene
  codificación wire de filas. Límites de preparación, input y metadatos, guards y
  cancelación se probaron; no se añadió pool/transporte/coordinador retry alternativo.
- Retirada FFI/Python: bridge, adapters, patch/build scripts y tooling activos se
  retiraron; sources importados siguen como proveniencia, no entrega prometida.
  Nuevos bindings requieren decisión explícita posterior a aceptación nativa.

La auditoría no autoriza borrar cambios desconocidos ni crear framework SQL/runtime.
Los resultados de esa historia no afirman aceptación del runtime Hotlap ni
exactly-once desde Fluss.
