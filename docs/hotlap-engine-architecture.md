# Arquitectura del motor incremental Hotlap

Detalle de diseño del [motor Hotlap](hotlap-engine.md).

## 1. Decisión arquitectónica

El motor incremental es **`hotlap-engine`**, con almacenamiento columnar propio y Arrow
en las fronteras de entrada/salida:

- Los lotes de entrada y salida usan arrays Arrow (`RecordBatch`); el estado
  interno de operadores usa estructuras columnares propias.
- Las claves/orden usan **`arrow::row::RowConverter`** (formato de fila
  byte-comparable → `Ord`/`Hash`).
- Los operadores usan kernels de **`arrow::compute`** (filter/take/concat/sort)
  más **lógica IVM propia** (consolidación, retracciones, epochs, watermarks).
- Los operadores incrementales mantienen estado propio entre actualizaciones.

El motor usa `arrow::row` para ordenar claves y `arrow::compute` para los kernels
de columnas; el estado incremental se conserva en estructuras columnares propias.

## 2. Arquitectura de crates

```
crates/hotlap-core      contrato: Plan, ZSetBatch, ids, WatermarkSpec, IncrementalCore | arrow
crates/hotlap-engine    motor columnar (impl IncrementalCore)                         | arrow, arrow-row, arrow-compute
crates/hotlap           fachada Hotlap::open_with(core) sobre el contrato            | hotlap-core
crates/hotlap-connectors / hotlap-sql   fronteras nativas Arrow                     | arrow
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
    pub batch: RecordBatch,  // each row is a record
    pub diff: ArrayRef,      // Int64 multiplicity; negative values retract
}
```

- La **identidad de una fila es la fila completa**: todas las columnas participan
  en la agrupación.
- `ZSetBatch::new` rechaza una columna `diff` cuya longitud no coincide con las
  filas.
- El **tiempo de evento** es una columna `Int64` del `RecordBatch` cuando la entrada
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

`EngineCore` (`crates/hotlap-engine/src/core.rs`) es la implementación propia.
Cada vista compila a un **grafo de operadores persistente** al hacer
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
