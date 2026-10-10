# Routing y propiedad Arrow en escrituras

Alcance: optimización Rust INSERT log finita/continua y evaluación separada de
upsert/delete KV. Verificación funcional DEBUG/ocho jobs. Esto cubre materialización
y corrección, no throughput sostenido; evidencia RSS está en
[perfiles nativos](native-profile-plan.md).

## Límites de ruta y materialización

```text
DataFusion input → schema/null validation → input backing lease
  → blocking worker + pool-admitted routing scratch
  → client schema normalization + authoritative destination grouping
  → contiguous slice / interleaved Arrow take
  → client byte-target slices → PrebuiltRecordBatchBuilder
  → IPC/statistics/compression → sender/retries → flush/ACK
```

El sink log ya no llama append por filas ni reconstruye columnas Arrow mediante
`RowAppendRecordBatchBuilder`. Un `ColumnarRow` reutilizable del cliente lee claves
de routing; no convierte cada campo payload en una nueva columna Arrow.

| Límite | Asignación/copia | Evidencia |
| --- | --- | --- |
| Normalización de esquema compatible | Clona referencias de columnas, no payload | Pruebas cliente existentes de normalización/tipos/NULL |
| Normalización de codificación sin pérdida | Casts Arrow pueden materializar; retainer llamador admite buffers resultantes | Misma normalización existente antes de agrupar; callback cubre casts |
| Destino homogéneo/contiguo | Slice comparte buffers respaldo | Nueva prueba cliente punteros/NULL/nested |
| Destino intercalado | Un Arrow take por destino, conserva orden relativo de entrada | Nueva prueba semántica; comparación nativa por filas activa de 96 filas cubre cinco grupos old2/new3 |
| Grupo de entrada sobredimensionado | Búsqueda binaria del tamaño lógico de slices y envío por bytes objetivo; respaldos originales siguen retenidos | 20 valores de 200.000 bytes divididos en cuatro slices con punteros idénticos; prueba activa entrada 4 MiB/buffer cliente 2 MiB |
| Lote wire | IPC/estadísticas/compresión y bytes codificados siguen materializándose | Pruebas builder/tamaño/estadísticas/tipos; no se afirma zero-copy integral |

Gather está justificado para filas intercaladas: usar lotes singleton preconstruidos
los lotes singleton multiplicarían framing IPC/requests. Grupos mantienen índices
en orden de entrada; no se garantiza orden global entre destinos. Se procesan según
primera aparición del destino. Índices de fila validan límite i32 de Fluss antes de
convertir a u32, sin copia adicional de índices gather.

Agrupar llama a `WriterClient::assign_bucket`, que resuelve conteos efectivos por
partición. `send_assigned` reutiliza assigner y el mismo snapshot Cluster inmutable
para enqueue; no vuelve a hashear una clave representativa contra layout nuevo.
Comportamiento nativo sticky/round-robin/hash, admisión accumulator, reintentos y
propagación ACK/error siguen en cliente. Metadatos desconocidos de partición tras
rescale fallan en vez de adivinar default de tabla.

## Contabilidad del pool y cliente

El conector reutiliza owners buffer `resources::reserve_batch` para entradas log y
buffers cast/gather materializados. Reservas sobreviven drop worker/query mientras
cliente retiene buffer; slices conservan lease de lote original y gathers tienen
leases independientes. Otros providers/VALUES reciben la misma admisión sink, sin
suponer que existan leases source.

Cliente nativo expone hint conservador de scratch routing: índices filas, metadatos
posibles destino y bytes respaldo claves seleccionadas. Sink admite hint contra pool
DataFusion suministrado durante worker. Antes de rescale, particiones sin resolver
pueden volver conservadora la allowance; después, conteos efectivos cacheados limitan
destinos posibles. No es otro pool ni asignador Arrow.

`append_arrow_batch_with_retainer` es hook de propiedad recursos alrededor de lotes
Arrow materializados. Callback debe preservar esquema, valores, conteo y orden.
`append_arrow_batch` nativo usa misma ruta con retención identidad. No queda ruta
antigua primera-partición/bucket ni writer alternativo. Admisión cast/gather ocurre
**después** de asignar Arrow y no evita picos. Puede haber errores tras enviar grupos
anteriores; no constituyen rollback de sentencia.

Permisos batch del cliente incluyen ahora hint lógico de tamaño Arrow y framing
existente; no tratan lotes preconstruidos como registros de cero bytes. Estimación
slice usa `ArrayData::get_slice_memory_size` de Arrow, con fallback conservador a
bytes respaldo para layouts no admitidos. Almacenamiento hijo anidado también puede
elevar estimación. Admisión request/buffer sigue estimada, no limita RSS; scratch
encoder y buffers codificados reales requieren perfil. Singleton puede exceder
objetivo de lote, pero debe pasar admisión buffer y estimación de tamaño request
sin comprimir.

## Decisión KV

KV no usa formato de lote Arrow-log. `UpsertWriter::upsert` codifica claves PK/bucket
 y bytes de fila requeridos; `delete` emite claves sin valores. ColumnarRow no
proporciona bytes KV ya codificados, por lo que hace falta codificación por filas.
Encoders/accumulator existentes reutilizan buffers propios. Sustituir por append
log violaría protocolo/semántica PK/delete.

Se conserva una vista de fila tipada reutilizable para acciones KV/MERGE y reserva
worker existente. Preservar orden por clave, validación NULL/tipos, upsert de fila
completa, política delete y layouts antiguos/nuevos. Otra API batch KV exige medir
beneficio adicional a codificación wire requerida; no se añade writer duplicado ni
falsa afirmación zero-copy.

## Verificación registrada

- Biblioteca cliente: **831 aprobadas, 2 ignoradas**, incluidas pruebas nuevas
  contiguo/gather y byte-slice, más encoder/builder/null/nested/statistics/limiter.
- Biblioteca core: **23 aprobadas**, incluye prueba lease sink que retiene slice y
  gather independiente tras drop worker, luego libera ambas reservas.
- Cuatro pruebas `write_sql` native-sni: INSERT log/KV, fuente SQL/concurrencia/
  reejecución, regresiones DELETE/MERGE, presión entrada 4 MiB/buffer 2 MiB y rechazo
  pool pequeño, particiones mixtas/rescale, confirmación singleton continua previa
  a EOF y cancelación. Prueba rescale compara pertenencia exacta a buckets, orden
  por bucket y valores NULL con 96 escrituras nativas por fila, y comprueba pool
  vuelve a nivel previo a INSERT tras ACK. Resultados SELECT KV retenidos previamente
  conservan leases fuente legítimos; INSERT no debe liberarlos.
- Clippy core/cliente con `-D warnings`; formato de paquetes.

Comandos desde la raíz del repositorio:

```sh
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p fluss-datafusion --locked --lib
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR="$PWD/target" cargo test --manifest-path clients/rust/Cargo.toml -p fluss-rs --locked --lib
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 uv run --no-project --env-file ../lab/.env cargo test -p fluss-datafusion --locked --test write_sql -- --ignored --test-threads=1
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo clippy -p fluss-datafusion --all-targets --all-features --locked -- -D warnings
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR="$PWD/target" cargo clippy --manifest-path clients/rust/Cargo.toml -p fluss-rs --all-targets --locked -- -D warnings
```

La cobertura de KV disperso en streaming, espera idle mayor que el ACK timeout,
pérdida de ACK/cancelación saturada, confirmaciones estructuradas y aceptación
de reinicio se registra por separado en
[aceptación de escrituras](streaming-write-acceptance.md),
[observabilidad de escritura](write-observation-contract.md) y
[verificación de presión](write-pressure-verification.md). La regresión de
streaming existente no demuestra por sí sola toda esa aceptación.
