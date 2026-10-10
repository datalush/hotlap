# Hallazgos técnicos de la auditoría Rust

Instantánea auditada: 2026-10-04; base `d06288a` (MERGE), `f66e90c`
(INSERT/DELETE), `89e111b` (batch/streaming), con cambios FFI sin commit encima.
Son observaciones del árbol histórico, no estado del código actual. **Confirmado**
indica comportamiento demostrado por el código; **por verificar** marca riesgo que
entonces requería escenario antes de llamarlo bug o retirar implementación.

## Planificación DML y distribución del sink

La ruta directa sustituyó temporalmente planificación por defecto en DELETE/MERGE
por introducción FFI. Se confirmó llamada a `DefaultPhysicalPlanner`
(`kv_table.rs:275–277`, `merge.rs:281–290`, diff sin commit); no se concluyó que
todo optimizador físico quedara apagado. Decisión requerida: recuperar composición
Rust sin llamadas recursivas; probar planner personalizado, alias/UDF y todas las
particiones de entrada.

El sink incorporaba coalesce explícito para un grafo FFI opaco (`write.rs:115–124`).
`DataSinkExec` declara `Distribution::SinglePartition` y ejecuta partición cero
(`datafusion-datasource/src/sink.rs:277–287,339–354`); la planificación nativa debe
hacer cumplir ese requisito antes de retirar coalesce. Ejecutar un grafo sin
optimizar no demuestra esa garantía.

**Observación precisa de DataFusion 55.1.0:**
`SessionState::create_physical_plan` primero optimiza el plan lógico y utiliza el
`QueryPlanner` de la sesión (`datafusion/src/execution/session_state.rs:777–784`).
`DefaultPhysicalPlanner::create_physical_plan` crea y optimiza físicamente el grafo
inicial (`datafusion/src/physical_planner.rs:154–171`) con optimizadores físicos de
sesión. La sustitución directa no desactivaba todos los optimizadores físicos, pero
sí omitía el planner de sesión y la optimización lógica de esa entrada. Importaba
para pruning de proyección y extensiones personalizadas.

## Escritura Arrow y routing efectivo

La ruta sink log reconstruía entradas compatibles mediante builders por fila. El
cliente debía ofrecer la ruta preconstruida donde fuera válida, sin prometer eliminar
codificación wire requerida. `append_arrow_batch` agrupaba con el conteo de buckets
de tabla (`append.rs:199–231`), mientras `WriterClient::assign_bucket` usaba el
conteo efectivo vivo por partición (`writer_client.rs:175–215`). Agrupación y envío
debían compartir layout efectivo: conteos distintos podían mezclar filas que luego
ruteaban distinto. La implementación existente usaba routing por fila; no debía
cambiarse sin probar layouts antiguos/nuevos.

El append batch asumía la partición de la primera fila y usaba `take_rows` para
varios grupos bucket. Había que validar entrada homogénea o agrupar correctamente
en el cliente, con slices contiguos y gather justificado; no replicar routing en
bindings.

## Retención de lectura y proyección KV

Las colas log decodificadas no estaban cubiertas por la reserva del lote emitido.
`scanner/batches.rs:24–59` producía hasta 100 lotes por poll con límite blando de
64 MiB, contado tras decodificar; el último fetch podía superar el cap, que no era
permiso previo de pool. `reader.rs:493–546` y `scan.rs::ActiveReader::Streaming`
(`scan.rs:130–151`) retenían lotes decodificados. `scan.rs:273–286` cobraba solo el
lote de salida, y `scan.rs:203–207` liberaba la reserva en el siguiente pull aunque
un operador/collector pudiera conservar el lote anterior. No era contabilidad de
vida útil de todos los buffers ni límite RSS. Permisos de descarga remota en
`remote_log.rs::PrefetchBytesPermit` limitaban otro recurso (disco/concurrencia).

KV construía filas Arrow completas antes de proyectar:
`batch_scanner.rs::value_records_to_record_batch` (368–392). Se podía evitar
materializar campos no seleccionados, preservando evolución de esquema y NULL.
Las páginas de ~1 MiB de `kv_scanner.rs` no limitaban estrictamente memoria Arrow
decodificada; `kv_scan.rs:129–181` liberaba reserva anterior en pull y reservaba
después de decodificar, con la misma salvedad de retención que log.

## Plazos, scratch MERGE y progreso

El deadline ACK comenzaba después de conexiones, tabla/metadatos y trabajo de claves
MERGE (`write.rs:257–408`). Debían distinguirse plazos de preparación, red, enqueue
y ACK con mecanismos existentes; input idle no tenía por qué ser un fallo de
escritura.

MERGE reservaba scratch según dos veces los bytes del lote completo más margen por
fila y copiaba claves codificadas al conjunto persistente (`write.rs:380–405`). El
estado de claves duplicadas retenido era legítimo, pero scratch podía rechazar payload
grande no-clave. La estimación debía basarse en claves seleccionadas y su overhead.

El progreso (`log_progress.rs`, `scan.rs:289–305`) emitía lotes ofrecidos y permitía
huecos por pruning. Debía observar rangos iniciales/vacíos/podados e identidad de
ejecución sin esperar al primer lote; no era checkpoint del motor.

Capturas usaban puntero `TaskContext` y generaciones para particiones repetidas
(`offsets.rs:67–85`); el supuesto de reutilización solapada del mismo contexto no
estaba establecido. Había que documentar identidad de ejecución y probar solapamiento
con un mismo contexto, además de contextos nuevos y reuso secuencial.

## Resultados parciales, cancelación y catálogo

El sink devolvía conteo final; tras fallos posteriores a lotes confirmados no había
resultado parcial estructurado (`write.rs:351–435`). Debía conservarse el contrato
SQL del conteo y exponer observaciones de confirmación veraces, sin sugerir rollback.

Conexión dedicada de escritura y abort síncrono aislaban operaciones
(`write.rs:138–164`, `writer_client.rs:263–280`); abortar sender no era rollback ni
esperar su terminación. Se debía preservar aislamiento y verificar limpieza acotada
con buffers saturados/ACK perdido antes de compartir writers.

El catálogo snapshot cronometraba cada RPC de nombres, pero abrir provider no usaba
el mismo wrapper (`catalog.rs:25–51,102–120`). Había que aclarar plazo por operación
vs. descubrimiento total y deadlines RPC del cliente; no hacía falta un servicio
global automático de catálogo.

## Filtros exactos y poda

Los filtros SQL residuales son intencionales: `filter.rs` poda lotes completos de
forma conservadora y `log_table.rs::scan` deja filtros exactos/límites globales a
DataFusion. Retirar residual para evitar duplicación aparente alteraría resultados.
La poda de particiones reduce descubrimiento; no filtra filas.

## Registro histórico de código transitorio

El registro original marcó estos elementos con etiquetas de planificación. Se
retiran esas referencias y se conserva su contexto/disposición técnica:
planner default y coalesce motivados por FFI;
`FlussPlanner`/`FlussExtension`; `OpaqueQueryPlanner` y registro/tokens/codecs;
`session_with_runtime`, `ResourceProvider`, `RuntimePlan` y cápsula/adaptador de
memoria; patch/build script/markers de host; ruta append por filas y adaptador
`merge.rs::InputPlan`. Las disposiciones y resolución posterior se documentan en
[historial de auditoría](rust-implementation-history.md). No eran una instrucción
para borrar código sin revisar semántica.

La wrapper de métricas, validación particiones/offsets, ACK del writer, conversión de
esquema y pruning conservador no eran «slop» por ser específicos del conector.
Helpers pequeños de error no justificaban por sí solos un framework genérico;
dividir `write.rs` debía seguir límites coherentes, no capas que solo reenvían llamadas.
