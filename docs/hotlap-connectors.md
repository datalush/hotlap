# Hotlap — conectores: SPI propio, source Fluss y adaptador DataFusion

- Fecha: 2026-10-07
- Estado: implementado (SP2), tests verdes
- Alcance: contrato de conectores propio del motor, source Fluss y adaptador DataFusion
- Crate: `hotlap-connectors` (kernel `crates/hotlap` sin cambios)
- Plan: SP2 (`2026-10-07-hotlap-sp2-connectors-spi-fluss.md`)

## 1. Propósito

`hotlap-connectors` aporta un **contrato de conectores propio del motor** (SPI),
una **implementación de `Source` sobre Fluss** y un **adaptador DataFusion** que
expone cualquier `Source` como tabla consultable. El kernel `crates/hotlap`
permanece ajeno a estos tipos: la frontera entra por `Pipeline`/`EngineHandle`.

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

Métodos del trait `Sink` (`Send + Sync`), solo **forma** en SP2 (sin implementación
Fluss): `write(&self, changes: ChangeStream) -> Result<(), ConnectorError>`,
`commit(&self)`, `abort(&self)`. El 2PC real llega en SP4.

`ConnectorError` (`error.rs`) distingue `Fluss(String)`, `Arrow(String)`,
`Unsupported(String)` e `Infrastructure(String)`.

## 3. Modelo Arrow-first, kernel Arrow-free

- El **plano de datos del SPI es Arrow**: `Source` produce `RecordBatch` y
  `Sink`/`ChangeStream` consumen `ChangeBatch` del kernel.
- `crates/hotlap` **no arrastra Arrow**; la conversión Arrow → kernel vive en
  `convert.rs` (`ensure_supported` + `to_change_batch`), con mapeo
  `Int64→Scalar::I64`, `Utf8→Scalar::Str`, `Boolean→Scalar::Bool` y `null→Scalar::Null`.
  Tipos no representables se **rechazan explícitamente** (`Unsupported`).
- El kernel interno columnar (SoA con `Columnar`/`Columnation`) es un
  **sub-proyecto posterior**, separado de este SPI. Arrow no implementa esos
  traits; el relayout Arrow↔columnar sería por columnas, no serialización.

## 4. Seam A1 (duplicación Fluss acotada)

Decisión **A1**: el connector Fluss se implementa **directo sobre `fluss-rs`**,
aislado tras un seam en `fluss/log_reader.rs` (open/subscribe/poll de un bucket,
`Rec { timestamp, offset, row }`) más el ensamblado de lotes en
`fluss/stream.rs` y `fluss/assemble.rs`. `crates/fluss-datafusion` **no se toca**.

La extracción de una capa Fluss compartida entre ambos integradores queda
**pendiente** como follow-up, a decidir **con evidencia** de las dos
implementaciones (no antes).

## 5. Event-time

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
- El bucle usa `tokio::select!` entre el **stream del source** (merge de splits
  vía `select_all` e ingestión por lote) y los **comandos**.
- `EngineHandle` expone `snapshot(view)`, `late_dropped(input)` y `shutdown()`
  (este último hace join del hilo).

## 7. Adaptador DataFusion

`datafusion/provider.rs` implementa `SourceTableProvider` sobre un
`Arc<dyn Source>`:

- `scan` proyecta el esquema y crea una `PartitionStream` por split, montadas en
  `StreamingTableExec`.
- La **boundedness** del plan sigue a `Source::is_unbounded()`
  (`Bounded` vs `Unbounded`).
- **Caveat:** el cursor es único. El reparto live de un mismo split entre varias
  particiones/consultas consumidoras queda **fuera de SP2**.

## 8. Estado

`SourceState` es **in-memory y serializable** (`serde`), pensado para
checkpoints futuros. La **persistencia** (y el arranque desde checkpoint) llega
en **SP4**; en SP2 el estado se mantiene vivo en el proceso.

## 9. No-goals

- 2PC real de escritura (solo forma de `Sink`).
- `Sink` sobre Fluss.
- Tap de changelog.
- Persistencia de estado / recuperación desde checkpoint.
- Kernel columnar (SoA).

## 10. Verificación

```bash
cargo fmt --all -- --check
cargo clippy -p hotlap-connectors --all-targets -- -D warnings
cargo test -p hotlap-connectors
cargo test -p hotlap
```

Cobertura: unit (`convert`, `source`) e integración
(`pipeline_differential.rs` — paridad vs recomputación completa,
`engine_lifecycle.rs` — arranque/snapshot/shutdown, `adapter.rs` — tabla
DataFusion y boundedness). `fluss_live.rs` es un test **ignored** (requiere un
broker Fluss vivo).
