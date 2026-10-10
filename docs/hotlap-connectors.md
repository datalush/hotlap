# Hotlap — conectores: SPI propio, source Fluss y adaptador DataFusion

- Registro: 2026-10-07
- Estado: arquitectura histórica; ver contratos enlazados para semántica actual.
- Alcance: contrato de conectores propio del motor, source Fluss y adaptador DataFusion
- Crate: `hotlap-connectors` (el kernel `crates/hotlap` conserva su propia representación)

## 1. Propósito

`hotlap-connectors` aporta un **contrato de conectores propio del motor** (SPI),
una **implementación de `Source` sobre Fluss** y un **adaptador DataFusion** que
expone cualquier `Source` como tabla consultable. El runtime de Hotlap usa ese
SPI para `Pipeline`/`EngineHandle`; la integración `fluss-datafusion` es un
provider separado y su aceptación no certifica este runtime.

## 2. Contrato `Source`/`Sink`

Todo el SPI vive en `source.rs` y `sink.rs`. Tipos:

| Tipo | Definición | Rol |
| --- | --- | --- |
| `SplitId` | `i32` | identidad de split (v1: un bucket Fluss) |
| `Offset` | `i64` | offset de log dentro de un split |
| `Split` | `{ id: SplitId, start: Offset }` | unidad de lectura y su offset inicial |
| `SourceState` | `{ offsets: BTreeMap<SplitId, Offset> }` | offsets reanudables por split, serializables |
| `SourceBatch` | `{ batch: RecordBatch, base_offset: Offset }` | lote Arrow ordenado (incluye `_event_time` si existe) |
| `SourceStream` | `Pin<Box<dyn Stream<Item = Result<SourceBatch, ConnectorError>> + Send>>` | stream de lotes de un split |
| `ChangeStream` | `Pin<Box<dyn Stream<Item = Result<ChangeBatch, ConnectorError>> + Send>>` | stream de cambios (Z-sets) hacia un sink |

Métodos del trait `Source` (`Send + Sync`):

| Método | Firma | Uso |
| --- | --- | --- |
| `schema` | `fn schema(&self) -> SchemaRef` | esquema Arrow completo (incluye `_event_time` si lo hay) |
| `splits` | `fn splits(&self) -> Result<Vec<Split>, ConnectorError>` | enumera splits y offsets de arranque |
| `read` | `fn read(&self, split: &Split) -> Result<SourceStream, ConnectorError>` | lectura ordenada de un split |
| `state` | `fn state(&self) -> SourceState` | estado reanudable actual |
| `event_time_column` | `fn event_time_column(&self) -> Option<usize>` | índice de la columna event-time (ms), si existe |
| `is_unbounded` | `fn is_unbounded(&self) -> bool` | ¿el source nunca termina? (por defecto `false`) |

El trait `Sink` (`Send + Sync`) ofrece `write(&self, changes: ChangeStream) ->
Result<(), ConnectorError>`, `prepare(&self)`, `commit(&self)`, `abort(&self)` y
las capacidades declaradas por el sink (`capabilities`, `accepts_retractions`,
`commit_redriable`). Ver `docs/hotlap-sink.md` y `docs/hotlap-sink-2pc.md`.

`ConnectorError` (`error.rs`) distingue `Fluss(String)`, `Arrow(String)`,
`Unsupported(String)` e `Infrastructure(String)`.

## 3. Representación Arrow y kernel columnar

- El **plano de datos del SPI es Arrow**: `Source` produce `RecordBatch` y
  `Sink`/`ChangeStream` consumen `ChangeBatch` del kernel.
- `crates/hotlap-core` define `ZSetBatch` como un `RecordBatch` y una columna
  firmada `diff`; el motor conserva estado interno columnar. El motor también
  depende de Arrow para expresiones y materialización de lotes.
- El conector valida tipos soportados y envuelve lotes Arrow como Z-sets. Los
  tipos no admitidos se rechazan explícitamente (`Unsupported`).

## 4. Integración Fluss

La integración Fluss de Hotlap se implementa **directamente sobre `fluss-rs`**, mediante
`fluss/log_reader.rs` (open/subscribe/poll de un bucket,
`Rec { timestamp, offset, row }`) y ensamblado de lotes en
`fluss/stream.rs` y `fluss/assemble.rs`. `crates/fluss-datafusion` **no se toca**.

No hay una capa Fluss compartida entre ambos integradores: este runtime consume
el SPI propio de Hotlap, mientras que `fluss-datafusion` implementa providers
nativos para DataFusion.

## 5. Tiempo de evento

El source Fluss añade la columna **`_event_time`** (`Int64`, milisegundos),
tomada del `timestamp` que el broker adjunta a cada registro. La columna se
agrega en `assemble.rs` (`with_event_time`) y su índice se reporta por
`Source::event_time_column()`. El `Watermark` de `Pipeline` solo declara la
`lag`; en `setup` el índice `time_col` se **deriva del source**
(`event_time_column()`), y si el source no tiene event-time se rechaza con
`Unsupported` en lugar de asumir una columna.

## 6. Runtime

- Un **hilo dedicado** (`runtime/engine.rs`) posee `Hotlap`; `EngineHandle`
  (`runtime/handle.rs`) habla con él por canal de comandos.
- El bucle usa `tokio::select!` entre el **stream de fuente** (mezcla de splits
  vía `select_all` e ingestión por lote) y los **comandos**.
- `EngineHandle` expone `snapshot(view)`, `late_dropped(input)` y `shutdown()`
  (este último libera el checkpointer, cierra los sinks, une el hilo y propaga
  los fallos de cierre en vez de devolver `Ok` incondicionalmente).

## 7. Adaptador DataFusion

`datafusion/provider.rs` implementa `SourceTableProvider` sobre un
`Arc<dyn Source>`:

- `scan` proyecta esquema y crea una `PartitionStream` por split, montada en
  `StreamingTableExec`.
- El **alcance acotado** del plan sigue a `Source::is_unbounded()`
  (`Bounded` vs `Unbounded`).
- **Límite:** el cursor es único. El reparto live de un mismo split entre varias
  particiones/consultas consumidoras no está soportado.

## 8. Estado

`SourceState` es **en memoria y serializable** (`serde`). Persistencia y arranque
desde checkpoint se describen en [durabilidad](hotlap-durability.md) y
[recuperación](hotlap-recovery.md).

## 9. Fuera de alcance

- 2PC distribuido con Fluss.
- Reparto de un cursor entre consumidores concurrentes.

## 10. Cobertura registrada

Cobertura: pruebas unitarias (`convert`, `source`) e integración
(`pipeline_differential.rs` — paridad vs recomputación completa,
`engine_lifecycle.rs` — arranque/snapshot/shutdown, `adapter.rs` — tabla
DataFusion y alcance acotado/no acotado). `fluss_live.rs` está ignorada por
defecto y requiere un broker Fluss activo.
