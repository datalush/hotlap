# Flujos de datos y recursos en la auditoría Rust

Observaciones técnicas de la instantánea histórica. Rutas y símbolos describen el
árbol auditado entonces; no son contrato del runtime actual.

## Inventario del cliente y bindings

Las rutas del cliente están bajo `clients/rust/crates/fluss/src/`.

| Área | Responsabilidad observada en la instantánea |
| --- | --- |
| `client/connection.rs`, metadatos | Conexiones/metadatos compartidos y vida writer; conexión de escritura dedicada aislaba cancelación/flush |
| `client/table/scanner/{api,builder,runtime,subscriptions,status,requests,responses,fetch,polling,batches,records,poll_timing}.rs` | Suscripción/fetch/poll y lotes; auditar límite cola/poll decodificado, no duplicar scanner en conector |
| `client/table/log_fetch_buffer.rs`, `reader.rs` | Fetch raw/completo, recorte rango y lotes bufferizados; contabilizar colas decodificadas |
| `client/table/remote_log.rs` | Concurrencia de descarga/permisos disco; recurso distinto de reserva Arrow DataFusion |
| `client/table/{scan,kv_scan}.rs` | RPC scan, buffers/páginas y conversión; preservar ownership/cancelación/contexto |
| `client/table/append.rs`, `table.rs` | `append` por fila y `append_arrow_batch`; batch solo log, KV usa `UpsertWriter` |
| `client/write/{writer_client,accumulator,batch}.rs` | Routing efectivo y vida accumulator/completion; retry/ACK/encoding wire pertenecen cliente |
| `client/table/partition.rs`, schema/bucket/metadata | Descubrimiento/routing y vistas metadatos; evitar snapshot obsoleto durante append |
| `client/row.rs`, `row/` | Conversión tipada/compactada; evitar parser duplicado y preservar evolución field IDs |
| `client/{error,metrics}.rs`, `client/rpc/` | Causas, métricas, multiplexación/red; no crear transporte paralelo en conector |

El inventario original también cubrió bindings/bridge genérico de esa instantánea,
retirados después del workspace activo. No son APIs a reintroducir aquí.

| Área | Responsable | Disposición / evidencia |
| --- | --- | --- |
| `client/connection.rs`, metadatos | Conexiones, metadatos compartidos y ciclo de vida del writer | Mantener. La conexión dedicada a escritura aísla cancelación/flush; preservar aislamiento al reducir conexiones |
| `client/table/scanner/{api,builder,runtime,subscriptions,status,requests,responses,fetch,polling,batches,records,poll_timing}.rs` | Suscripción/fetch/poll del cliente y producción de lotes | Mantener protocolo/estado; auditar límites de poll decodificado y cola, no duplicar scanner en conector |
| `client/table/log_fetch_buffer.rs`, `reader.rs` | Fetch crudo/completo, recorte de rango y lotes bufferizados | Mantener rangos/limpieza; contabilizar explícitamente colas decodificadas |
| `client/table/remote_log.rs` | Concurrencia de descarga, permisos de disco, limpieza/reintentos | Mantener permisos reales; no confundir disco con RAM Arrow |
| `client/table/kv_scanner.rs`, `batch_scanner.rs`, `read_context_resolver.rs` | Páginas snapshot por bucket y decodificación consciente del esquema | Mantener corrección del protocolo; evaluar aparte decodificación/proyección de filas completas |
| `client/table/{append,upsert,partition_getter}.rs` | APIs de escritura fila/lote y selección física | Mejorar agrupación Arrow aquí antes de usarla en sink |
| `client/write/{writer_client,bucket_assigner,accumulator,batch,sender,broadcast}.rs` | Routing autoritativo, admisión de cola, codificación, reintentos, ACK/errores | Mantener una implementación; sink consume ACK, no crea otra pila transporte/reintentos |
| `record/arrow.rs`, decodificación/codificación KV y column writers | Conversión wire/Arrow y normalización de esquema | Mantener materialización requerida; evitar reconstrucción Arrow→fila→Arrow innecesaria en lotes log compatibles |
| `crates/fluss-datafusion-python/src/{lib,extension}.rs`, helpers Python | Export/config de bindings y extensión experimental de sesión | Rediseño histórico; no es requisito para aceptación Rust |
| `crates/datafusion-ffi-ext/src/{memory,provider,runtime,registry}.rs`, `tools/datafusion-python-ext.patch`, `tools/build-ext-host.sh` | Adaptadores experimentales de recursos externos y planes opacos | Inventariar como transitorios, no arquitectura central; resolución histórica y retiro de piezas sustituidas |

Esto agrupa subsistemas; no afirma una revisión completa de cada RPC o formato.
Las rutas críticas se inspeccionaron directamente; mediciones profundas de
asignación se registran como evidencia posterior.

## Flujos de lectura y escritura observados

### Lecturas log

```text
archivo remoto/RPC → fetch crudo completo → decode Arrow cliente
  → collect_batches Vec<ScanBatch> → reader acotado o VecDeque streaming
  → prepare_output_batch conector → proyección/vista → operadores DataFusion
```

- `scanner/batches.rs:24–59` producía hasta 100 lotes por poll con cap **blando**
  de 64 MiB. Tamaño crudo/decodificado se contabilizaba tras decode; el último
  fetch podía excederlo. No era reserva previa del pool.
- `reader.rs:493–546` retenía lotes decodificados del poll; streaming también
  almacenaba en cola `scan.rs::ActiveReader::Streaming` (`scan.rs:130–151`).
- `scan.rs:273–286` cobraba solo el lote elegido para salida tras decodificar.
  Proyección nativa clonaba referencias, no reconstruía los datos.
- `scan.rs:203–207` liberaba reserva en el siguiente pull bajo el supuesto de
  consumo del lote previo; un operador/collector downstream podía retenerlo. No
  era contabilidad de vida útil de todos los buffers. Operadores podían tener
  reservas separadas, pero eso no validaba el supuesto en general.
- Permisos de `remote_log.rs::PrefetchBytesPermit` y admisión de descarga
  (`reserve_bytes`, ajuste por tamaño real antes de escribir chunks) limitaban
  disco/concurrencia, recurso distinto.

### Lecturas KV

```text
página snapshot por bucket → registros KV compactos → decoder de esquema
  → RowAppendRecordBatchBuilder → lote Arrow completo → proyección
  → reserva conector → operadores DataFusion
```

`batch_scanner.rs::value_records_to_record_batch` (368–392) decodificaba registros
y construía columnas Arrow por fila antes de proyectar. Esa conversión de formato
fila no era transporte zero-copy de columnas Arrow existentes. Podía evitarse crear
campos no seleccionados, preservando evolución esquema/NULL. `kv_scanner.rs` pedía
páginas de ~1 MiB, sin límite estricto a memoria Arrow decodificada. `kv_scan.rs:
129–181` liberaba reserva anterior en pull y reservaba tras decodificar, con la
misma salvedad de retención que en log.

### Escrituras

```text
lote DataFusion → validación/reserva conector → spawn_blocking
  → vista ColumnarRow reutilizable por fila → routing cliente
  → accumulator + permiso memoria → codificación fila/Arrow → sender/reintentos
  → flush/ACK → conteo confirmado (final solo al EOF)
```

- `write.rs::FlussWriter::enqueue` usaba una vista de fila tipada reutilizable;
  `RecordBatch::clone` y proyección de acción MERGE no copiaban payload.
- Para filas log, `record/arrow.rs::RowAppendRecordBatchBuilder::append`
  (256–261) escribía valores en column writers y creaba arrays Arrow: la ruta
  reconstruía Arrow→vistas de fila→columnas Arrow antes de codificar formato. Que
  las vistas apuntaran al lote original no la hacía zero-copy.
- `PrebuiltRecordBatchBuilder` (128–184) retenía arrays compatibles y evitaba esa
  reconstrucción. `prepare_append_record_batch` (860–917) clonaba columnas
  compatibles y casteaba codificaciones distintas admitidas; casts podían asignar.
- Aun esa ruta codificaba bytes wire: `ArrowLogWriteBatch::build`
  (`client/write/batch.rs:315–321`) materializaba/cacheaba `Bytes`. Compresión y
  wire encoding son distintos de reconstruir columnas Arrow evitablemente.
- El sink movía reserva de entrada al worker blocking (`write.rs:408–424`) como
  guard mientras conservaba lote. Colas codificadas usaban `MemoryLimiter` cliente
  separado; permisos correspondían a lotes incompletos hasta completar/abortar.
  Contabilizar dos veces arrays compartidos podía ser admisión conservadora, no
  evidencia de duplicar asignación física.
