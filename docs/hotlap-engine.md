# Hotlap — `hotlap-engine`: motor incremental Arrow-nativo

- Fecha: 2026-10-08
- Estado: implementado (rama `feat/columnar-engine`)
- Alcance: motor incremental **Arrow-nativo** propio que sustituye a
  `differential-dataflow` (DD); contrato `IncrementalCore`; operadores;
  frontera e integración.
- Diseño: `2026-10-08-hotlap-engine-design.md` (local, fuera del repo).
- Kata: `0qgh`.

> **Sustituye** la decisión previa de
> [núcleo incremental](hotlap-incremental-core.md), que adoptaba
> `differential-dataflow` + `timely`. Ese documento queda como historia.

## 1. Decisión

El motor incremental es **`hotlap-engine`**, **Arrow-nativo**:

- Las columnas son **arrays Arrow** (`RecordBatch`); los bordes
  (Fluss/SQL/sinks) ya son Arrow, así que la conversión de frontera es
  **cero-copia**.
- Las claves/orden usan **`arrow::row::RowConverter`** (formato de fila
  byte-comparable → `Ord`/`Hash`).
- Los operadores usan kernels de **`arrow::compute`** (filter/take/concat/sort)
  más **lógica IVM propia** (consolidación, retracciones, epochs, watermarks).
- **No hay dependencia de `differential-dataflow` ni de `timely`.**

Los spikes mostraron que un arrangement columnar propio gana, que el cuello de
botella de DD era su *trace*, y que un motor incremental propio reduce el trabajo
por clave de forma sustancial. Arrow 59 aporta justo lo que el motor necesita
(`arrow::row`, `arrow::compute`).

## 2. Arquitectura de crates

```
crates/hotlap-core      contrato: Plan, ZSetBatch, ids, WatermarkSpec, IncrementalCore | arrow
crates/hotlap-engine    motor Arrow-nativo (impl IncrementalCore)                    | arrow, arrow-row, arrow-compute
crates/hotlap           fachada Hotlap::open_with(core) sobre el contrato            | hotlap-core
crates/hotlap-connectors / hotlap-sql   frontera Arrow nativa (sin convert a Row)    | arrow
```

| Crate | Rol |
|---|---|
| `hotlap-core` | Contrato compartido, **sin motor**: `Plan` (IR), `ZSetBatch`, `InputId`/`ViewId`, `WatermarkSpec`, `Predicate`, `Scalar`, trait `IncrementalCore`. Depende de `arrow` (el Z-set es Arrow). |
| `hotlap-engine` | Implementación `EngineCore: IncrementalCore`. Módulos `keys`, `zset`, `time`, `arrange`, `ops/*`, `core`. |
| `hotlap` | API pública pura: `Hotlap` + `open_with(Box<dyn IncrementalCore>)`. **No** depende de `hotlap-engine` (los callers inyectan el core). |
| `hotlap-connectors` / `hotlap-sql` | Adaptan los bordes Arrow a la frontera y construyen el `EngineCore`. |

`hotlap-engine` reexporta los tipos de `hotlap-core` para comodidad; el motor no
filtra ningún tipo de terceros por la frontera.

## 3. Representación: Z-set Arrow

Un Z-set es un `RecordBatch` más una columna de multiplicidad firmada
(`crates/hotlap-core/src/batch.rs`):

```rust
pub struct ZSetBatch {
    pub batch: RecordBatch,  // las filas son el registro
    pub diff: ArrayRef,      // multiplicidad Int64; retracción = negativo
}
```

- La **identidad de una fila es la fila completa**: todas las columnas participan
  en la agrupación.
- `ZSetBatch::new` rechaza una columna `diff` cuya longitud no coincide con las
  filas.
- La **event-time** es una columna `Int64` del `RecordBatch` cuando el input
  declara watermark.

`consolidate` (`crates/hotlap-engine/src/zset.rs`) suma los `diff` de filas
idénticas y **descarta las filas cuya suma es cero**; la salida queda ordenada por
los bytes de las filas (determinismo).

## 4. Frontera: `IncrementalCore` y `EngineCore`

El trait del contrato (`crates/hotlap-core/src/core.rs`) habla solo en tipos del
motor:

```rust
pub trait IncrementalCore {
    fn register_input(&mut self, input: InputId) -> Result<(), CoreError>;
    fn build_view(&mut self, view: ViewId, plan: &Plan) -> Result<(), CoreError>;
    fn declare_watermark(&mut self, input: InputId, spec: WatermarkSpec) -> Result<(), CoreError>;
    fn push(&mut self, input: InputId, batch: &ZSetBatch) -> Result<(), CoreError>;
    fn snapshot(&mut self, view: ViewId) -> Result<ZSetBatch, CoreError>;
    fn late_dropped(&self, input: InputId) -> Result<u64, CoreError>;
    fn take_changes(&mut self, view: ViewId) -> Result<ZSetBatch, CoreError>;
    fn tap_view(&mut self, view: ViewId) -> Result<(), CoreError>;
}
```

`EngineCore` (`crates/hotlap-engine/src/core.rs`) es la implementación
DD-free. Cada vista compila a un **grafo de operadores persistente** al hacer
`build_view`:

- Un `push` propaga **solo el delta empujado** por los grafos que leen ese input;
  los operadores con estado **retienen su estado** entre pushes, así que el
  trabajo por push no crece con el historial acumulado.
- `snapshot` consolida los deltas de salida acumulados.
- `take_changes` drena los deltas acumulados para una vista `tap` (solo útil
  antes del primer push).

Antes del primer push se produce el **freeze** (congelado) de la configuración:
se rechaza mezclar inputs con y sin watermark, y una vista con ventana sin
event-time. Tras el freeze, las declaraciones (`register_input`, `build_view`,
`declare_watermark`, `tap_view`) fallan con "engine already running".

La fachada `Hotlap` (`crates/hotlap/src/engine.rs`) mantiene registros de
nombres → `InputId`/`ViewId`; los ids solo avanzan cuando el core acepta la
declaración. El core concreto se inyecta con `Hotlap::open_with`.

## 5. Tiempo: epochs, frontiers y retracciones

- **Watermark** (`crates/hotlap-engine/src/time.rs`): `max(event_ts) - lag`,
  acotada a cero y **monótona**. Cada observación avanza el watermark al menos a
  `max_ts - lag`; nunca retrocede.
- **Frontier**: por input, el mayor tiempo anunciado como completo. Un epoch está
  **settled** cuando todos los inputs lo han rebasado (`Frontier::settle`); el
  mínimo (`Frontier::min`) es el watermark de la vista.
- **Late data**: `EngineCore::push` descarta las **inserciones** (`diff > 0`) por
  debajo del watermark actual y las cuenta en `late_dropped`. Una **retracción
  nunca se descarta**: aplicarla siempre es necesario para no corromper el estado
  corriente abajo.
- **Retracciones**: se representan como `diff` negativo. Los arrangements suman
  los deltas de cada `(clave, payload)` y olvidan las entradas cuya suma llega a
  cero; los operadores de conteo/ventana propagan el changelog correspondiente
  (retraer el valor viejo e insertar el nuevo).

## 6. Operadores

`crates/hotlap-engine/src/ops/`:

| Operador | Implementación | Notas |
|---|---|---|
| `Filter` | `arrow::compute::filter_record_batch` | Filtra columnas de datos y `diff` juntos, de modo que las retracciones sobreviven si su fila pasa el filtro. `Predicate::{Eq,Gt}`. |
| `Project` | `arrow::compute` (`ArrayRef::clone`) | Selecciona/reordena columnas compartiendo arrays; O(nº de columnas). Conserva `diff`. |
| `GroupCount` | claves `arrow::row` + reduce incremental | Mantiene `clave → count`; actualiza **solo las claves tocadas** por el delta (coste O(delta), no O(keyspace)). Emite un changelog `(clave..., count)` con `diff` firmado: al cruzar a cero retrae el conteo viejo; una clave nueva inserta el suyo; una cambiada retrae el viejo e inserta el nuevo. |
| `Join` (inner equi) | claves `arrow::row` ambos lados | Cada lado acumula en un `KeyedArrangement`; cada `apply` recomputa el join y emite el changelog contra la relación anterior. La fila de salida es `left ‖ right`; las multiplicidades se multiplican. |
| `TumbleCount` | ventana tumbling sobre event-time | Cubetas abiertas en un `BTreeMap` por `window_start`; `ws = (event_ts / size) * size`. Emite cada ventana **una vez al cerrarse** (append-only) y la libera; las filas por debajo del watermark previo se cuentan como late y se descartan. |

El grafo de vista compone estos nodos; en un `Join` con fuentes que aún no tienen
esquema conocido, el lado pendiente se **bufferiza** hasta que ambos esquemas
están disponibles.

## 7. Integración (bordes)

`hotlap-connectors` y `hotlap-sql` ya son **Arrow nativos**: se elimina la
conversión a `Row` y se gestiona la columna de diffs. Adaptan a la nueva frontera
`ZSetBatch`/`RecordBatch` y construyen `Hotlap::open_with(EngineCore::new())`.

## 8. `differential-dataflow` eliminado

- Se **borra** `crates/hotlap/src/core/differential_dataflow/*` y las
  dependencias `differential-dataflow`/`timely`.
- El **oráculo** es la **recomputación completa** con un cómputo batch
  independiente: cada operador y combinación se contrasta incremental vs. batch,
  con retracciones y varios epochs.
- La corrección incremental (retracciones, frontiers, consolidación, joins) es
  **nuestra**; por eso la batería diferencial es exhaustiva.

Verificación de ausencia (cero referencias):

```bash
! grep -r "differential_dataflow\|differential-dataflow\|timely" crates/ Cargo.toml Cargo.lock
```

## 9. Límites (no-goals)

- **Join O(|L|·|R|) por delta**: cada `apply` materializa ambos arrangements y
  recomputa el join completo. Un join incremental por clave queda fuera de
  alcance (TODO explícito en `ops/join.rs`).
- **Estado sin poda (GC)**: coincide con el estado corriente, sin historia; los
  arrangements, las cubetas de ventana y las salidas acumuladas **no se podan**.
  No hay recolección de basura ni gestión avanzada de late-data.
- **Single-worker**: v1 sin exchange, sin *spill* y sin persistencia.
- **IR limitado**: solo los operadores de §6; tipos/agregados fuera del IR se
  rechazan con `Unsupported`. Sin hop/sliding/session.
- **Test de incrementalidad**: la garantía de corrección se apoya en la
  equivalencia *incremental ≡ recomputación completa*; no en una prueba formal.
  Pruebas: `arrange::incremental_matches_full_recompute`,
  `ops::group_count_changelog_consolidates_to_final_counts`,
  `join::join_matches_full_recompute_with_retractions`,
  `window::changelog_matches_full_recompute_across_windows`,
  `hotlap/tests/differential*`.

## 10. Verificación

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
! grep -r "differential_dataflow\|differential-dataflow\|timely" crates/ Cargo.toml Cargo.lock
```
