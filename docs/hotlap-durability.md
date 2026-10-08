# Hotlap — durabilidad: checkpoints, recovery, 2PC y vistas dinámicas

- Fecha: 2026-10-08
- Estado: implementado (SP4), tests verdes
- Alcance: durabilidad end-to-end del motor (estado, checkpoints, recuperación),
  coordinación 2PC de sinks con capacidades, y vistas materializadas creadas
  después de `START`.
- Motor: `docs/hotlap-engine.md`; conectores `docs/hotlap-connectors.md`; sink
  `docs/hotlap-sink.md`.
- Rama: `feat/durability`. Diseño:
  `2026-10-08-hotlap-durability-design.md` (local, fuera del repo).

> Código y comentarios en **inglés**; este documento en español.

## 1. Propósito

El motor (`hotlap-engine`, Arrow-nativo) es **en memoria**. SP4 añade
**durabilidad**: persistir y restaurar el estado, recuperarse de un fallo,
coordinar los sinks con consistencia y permitir **vistas creadas en caliente**.
El kernel `crates/hotlap` expone la frontera de estado (`StateBackend`); el
runtime (`hotlap-connectors`) orquesta checkpoints, 2PC y recovery.

## 2. Alcance y no-goals

**Dentro:** `StateBackend` durable (fallible, `fsync`); `EngineCore::checkpoint`
/ `restore` (snapshot versionado); Checkpointer (barrera + codec binario +
retención); protocolo **2PC** del sink + capacidades; recovery (checkpoint +
replay); dynamic views por retención de inputs.

**Fuera (no-goals):**

- **2PC en Fluss**: no se modifica Fluss ni su protocolo. Fluss no ofrece
  transacciones de sink; el motor solo puede orquestar 2PC real si un sink lo
  implementa. Con Fluss, el techo es **effectively-once** (upsert/PK,
  idempotente) o **at-least-once** (append).
- **Multi-nodo / cluster**: v1 es **single-process**; el `StateBackend` es local.
- Estado compartido entre procesos.
- Exactly-once estricto con sinks no transaccionales.
- Retención/GC avanzados de checkpoints más allá de "conservar los N más
  nuevos".
- Persistencia de la historia de inputs para dynamic views (ver §8).

## 3. `StateBackend` durable

El estado vive detrás de un trait de bytes opacos
(`crates/hotlap/src/state.rs`), de forma que la elección de persistencia no se
filtra al core:

```rust
pub trait StateBackend {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StateError>;
    fn put(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), StateError>;
    fn scan(&self, prefix: &[u8]) -> Result<Vec<StateEntry>, StateError>;
    fn list(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, StateError>;
    fn delete(&mut self, key: &[u8]) -> Result<(), StateError>;
}
```

- **Todas las operaciones son fallibles** (`StateError`): un backend real puede
  fallar en disco y debe **propagar** el error, nunca entrar en `panic`.
- **Claves con forma de namespace**: segmentos no vacíos separados por `/`
  (p.ej. `checkpoint/7/engine`). La clave vacía y los segmentos vacíos se
  rechazan (`EmptyKey`/`InvalidKey`).
- `scan`/`list` devuelven las claves con el prefijo dado, **ordenadas** por bytes
  (orden determinista); un prefijo vacío es "todo el store".
- `delete` de una clave ausente **no es error** (retención reintentable).

Implementaciones:

| Impl | Uso | Notas |
| --- | --- | --- |
| `BTreeMap<Vec<u8>, Vec<u8>>` | tests / in-memory | Mismo contrato; `scan` por rango. |
| `DurableStateBackend` (`state/durable.rs`) | persistencia local | **Un fichero por clave** bajo subdirectorios con prefijo. |

`DurableStateBackend`:

- Los segmentos de una clave se codifican en **hex** como componentes de path, de
  modo que un prefijo de namespace mapea a un subdirectorio y `scan`/`list` solo
  recorren el subárbol relevante.
- `put` es **atómico y duradero**: escribe `<name>.tmp`, hace `fsync`, renombra
  sobre el nombre final (`rename` atómico), y luego hace `fsync` del directorio
  padre y de los ancestros recién creados (`state/fsio.rs`). Un fallo de
  escritura **nunca** expone un valor a medio escribir, y un crash no pierde uno
  ya confirmado.
- `open(root)` crea el directorio raíz (con `fsync` de directorios) si no existe.

## 4. `EngineCore::checkpoint` / `restore`

El motor captura y reconstruye su estado como un **snapshot versionado**
(`crates/hotlap-core/src/snapshot.rs`):

```rust
pub struct EngineSnapshot {
    pub format_version: u32,          // ENGINE_SNAPSHOT_FORMAT_VERSION = 1
    pub epoch: u64,                   // epoch lógico al capturar
    pub frozen: bool,                 // ¿el esquema estaba congelado?
    pub inputs: Vec<InputSnapshot>,   // por input, ordenado por id
    pub views: Vec<ViewSnapshot>,     // por vista, ordenado por id
}
```

- **Por input**: id, esquema Arrow IPC (o `None` antes del primer push),
  `WatermarkSpec`, watermark monótono y contador `late`.
- **Por vista**: id, el `Plan` original (necesario para recompilar el grafo),
  `windowed`/`tapped`, la salida consolidada, el changelog `pending` del tap, y
  el estado de los operadores (arrangements por clave, buckets de ventana), uno
  por nodo del plan en pre-orden.
- Las tablas rectangulares se serializan como **Arrow IPC** (columnas de datos)
  más una columna `diff` firmada (`SnapshotTable`); el estado de operadores, como
  mapas ordenados `(bytes, valor)`.

`EngineCore::checkpoint(&self)` es **inmutable** (`&self`): captura sin mutar el
motor. `EngineCore::restore(&mut self, &EngineSnapshot)`:

- **Rechaza una versión de formato desconocida** (`Unsupported`).
- Decodifica todo en un motor **nuevo** y solo al final hace `*self = core`, de
  modo que un snapshot corrupto deja el motor **intacto** (todo-o-nada).
- La configuración de retención sobrevive, pero la **historia de inputs no se
  persistió**: al restaurar, la retención se **invalida** (§8), así que un
  `build_view` post-start se rechaza aunque la retención estuviera activa.

La fachada `Hotlap` (`crates/hotlap/src/engine.rs`) delega en el core; el borde
de conectores usa `hotlap.checkpoint()` / `hotlap.restore()`.

## 5. Checkpointer: barrera, codec binario y retención

`Checkpointer` (`crates/hotlap-connectors/src/runtime/checkpoint.rs`) escribe
checkpoints **coherentes y versionados** en un `StateBackend`:

- `Checkpointer::new(backend, retain)` fija el store y cuántos checkpoints
  conservar (`DEFAULT_RETAIN = 3`, recortado a ≥1).
- `with_sinks(Vec<Arc<SharedSink>>)` añade los sinks a la barrera 2PC.
- `resume_after(id)`: continúa la secuencia tras un checkpoint recuperado, para
  no sobrescribirlo.
- `take(engine, source)` captura y persiste un checkpoint nuevo (async).

**Formato binario.** El snapshot del motor y los offsets del source se codifican
con un **frame binario versionado** (`crates/hotlap-engine/src/core/ipc.rs`):

- Cabecera fija de 16 bytes: magic `HLSP` (4) + versión de frame (4) + longitud
  del payload (8, little-endian), seguido de un payload `bincode`.
- El decode **rechaza** magic incorrecto, versión desconocida, longitud que no
  coincide, bytes sobrantes o payloads por encima del límite (`MAX_FRAME_BYTES`),
  devolviendo error en vez de `panic`.
- El `format_version` del `EngineSnapshot` es un guardado adicional contra
  layouts incompatibles.

**Layout en disco** (namespace bajo `checkpoint/`):

| Clave | Contenido |
| --- | --- |
| `checkpoint/<id>/engine` | snapshot del motor (frame binario) |
| `checkpoint/<id>/sources` | `SourceState` (offsets, frame binario) |
| `checkpoint/<id>/valid` | marcador `1`: el checkpoint está completo |
| `checkpoint/latest` | id (8 bytes LE) del checkpoint nuevo más reciente |

**Publicación coherente.** Un checkpoint solo es visible cuando **todas** sus
partes están escritas: primero el cuerpo (`engine` + `sources`), luego el
marcador `valid`, y solo entonces el puntero `latest`. Una escritura
interrumpida **nunca** expone un checkpoint parcial como el actual.

**Retención** (`runtime/retention.rs`): tras publicar, se **podan** los
checkpoints más antiguos para conservar los `retain` más nuevos. El borrado solo
toca claves `checkpoint/<id>/...` (nunca `latest`) y es **idempotente**, así que
un store parcialmente podado se puede podar de nuevo.

**Disparo.** Periódico (config `CheckpointConfig { interval, backend, retain }`)
y **on-demand** (`CHECKPOINT` por el canal de comandos).

## 6. 2PC del sink y capacidades

El trait `Sink` (`crates/hotlap-connectors/src/sink.rs`) declara su garantía:

```rust
pub enum SinkCapabilities {
    Transactional, // prepare/commit/abort reales => exactly-once si todo va bien
    Idempotent,    // upserts por clave (PK) => replay seguro
    AtLeastOnce,   // escritura visible e irreversible => replay puede duplicar
}
```

y la forma 2PC: `prepare()` (por defecto no-op), `commit()`, `abort()`.
`capabilities()` por defecto es `AtLeastOnce`.

`SinkBarrier` (`runtime/sink_barrier.rs`) adapta el protocolo por capacidad:

- **`Transactional`**: `prepare` en la fase uno; `commit` en la fase dos; ante
  cualquier fallo previo a completar el commit, `abort` de los sinks preparados.
- **`Idempotent`**: no hay `prepare`; solo se hace `commit` (flush) en la fase
  dos. Reenviar tras un crash es seguro.
- **`AtLeastOnce`**: no se coordina; sus escrituras ya son visibles.

`SinkBarrier::around(capture)` ejecuta el orden **prepare → capture → commit**.
Si `capture` (snapshot + escritura del cuerpo) falla, o el `commit` posterior
falla, los sinks preparados se **abortan** y el error se propaga: ningún
checkpoint puede llegar a ser válido.

Detalles de corrección (`SharedSink`, `runtime/shared_sink.rs`):

- Cada sink es un `SharedSink`: un **mutex async** serializa `write` (tarea del
  sink) contra `prepare`/`commit`/`abort` (barrera). Así `prepare` **nunca**
  observa un `write` en vuelo. La tarea escribe un batch por llamada, de modo que
  el control entre batches no queda bloqueado detrás de un stream vivo.
- En la fase de commit se **flushean antes los idempotentes** y luego se
  confirman los transaccionales preparados. Si un idempotente fallara después de
  que un transaccional ya confirmó, el replay duplicaría; flush primero evita esa
  ventana.
- Un sink transaccional puede ser confirmado por la barrera **y** al cerrar el
  canal; `Sink::commit` debe tolerar ejecutarse **más de una vez**.

**Techo real con Fluss** (`fluss/sink.rs`): `FlussSink` elige el writer según la
tabla. Tabla con **primary key** → upsert → `Idempotent`; tabla **log** → append →
`AtLeastOnce`. En ambos modos se **rechazan retracciones** (`diff < 0`). Fluss
**no tiene transacción de sink**: `commit` es el `flush` esperado y `abort` es un
no-op intencional (los appends ya son visibles y los upserts son idempotentes).
Por tanto, **con Fluss el techo es effectively-once (PK) o at-least-once
(append)**; exactly-once real requeriría un sink `Transactional` (hoy ninguno) y
2PC en el almacén.

## 7. Recovery: checkpoint + replay

`Recovery` (`runtime/recovery.rs`):

1. **Cargar el último checkpoint válido**: se intenta `latest` primero; si su
   checkpoint no decodifica o no valida, se prueban los anteriores de más nuevo a
   más viejo. Como un checkpoint solo es visible cuando todas sus partes están
   escritas, uno legible es siempre **coherente**; un tip corrupto no aborta el
   arranque mientras quede un predecesor válido. Sin ningún checkpoint válido, es
   un **arranque limpio** (`None`).
2. **Restaurar** el motor con `EngineSnapshot` (`hotlap.restore`).
3. **Reabrir cada source** en los offsets capturados (`Source::resume`) y
   **replayar** desde ahí, alimentando el mismo circuito.

Invariante de replay: `SourceState` guarda el offset del **siguiente** registro a
leer (`records < offset` ya aplicados, `records >= offset` a replayar). Como los
checkpoints se toman **entre polls** del source, reabrir en `offset` **ni pierde
ni duplica** en la frontera.

**Retención explícita.** Si el source ya no puede servir un offset capturado
(p.ej. el log de Fluss podó registros por debajo del offset), `resume` devuelve
un error **explícito**: la recuperación falla ruidosamente en lugar de perder
registros en silencio.

## 8. Dynamic views (retención de inputs)

Permitir `CREATE MATERIALIZED VIEW` **después de `START`** requiere reconstruir
el estado como si la vista hubiera existido desde el principio. El motor
**retiene los Z-sets de entrada** aplicados, en su orden global de push
(`crates/hotlap-engine/src/core/retention.rs`):

- `set_input_retention(events)` (API: `Hotlap::set_input_retention`,
  `Session::with_input_retention`, runtime `EngineHandle`) fija un límite de
  **n deltas**; la retención está **apagada por defecto** (un `build_view`
  post-start se rechaza). Cada delta retenido guarda también el watermark del
  input tras ese push, para reproducir una vista con ventana **exactamente**.
- Al construir una vista post-start, se compila un grafo nuevo y se **evalúa
  sobre los deltas retenidos** en orden; a partir de ahí se une al flujo vivo
  (`build_view` post-start).
- Un contador `truncated` se activa al descartar el delta más antiguo por
  exceder la capacidad: el log ya no cubre el inicio del run y un `build_view`
  post-start se **rechaza** (`Unsupported`) en lugar de devolver una vista
  truncada silenciosamente.
- Si `replay` se invoca con la retención apagada o truncada, devuelve error
  explícito.

**Aún en memoria.** La retención vive **solo en memoria** y **no sobrevive a un
restart**: la historia de inputs **no** se persiste en el checkpoint. Por eso
`restore` **invalida** la retención (§4): tras un recovery, el estado restaurado
es correcto pero un `build_view` post-start se rechaza hasta que la retención
vuelva a cubrir el run. La persistencia de la historia de inputs (o el replay
desde el source) es un follow-up fuera de SP4.

## 9. Testing

- **`StateBackend` durable** (`crates/hotlap/tests/state_backend.rs`): memoria y
  durable coinciden para la misma secuencia; persistencia a través de reapertura;
  namespace nuevo persistido; `put` atómico/sobrescritura; subdirectorios por
  prefijo; `list` ignora restos no-hex. Errores en
  `hotlap/tests/state_backend_errors.rs`.
- **Checkpoint/restore** (`crates/hotlap-engine/tests/checkpoint.rs`):
  `restore_then_continue_matches_no_restart` (differential). Codec en
  `tests/codec.rs`: round-trip por frame binario, versión desconocida rechazada,
  frames corruptos son errores (no `panic`).
- **Checkpointer** (`hotlap-connectors/tests/`): `checkpoint.rs`
  (`on_demand_checkpoint_is_coherent_and_readable`), `checkpoint_retention.rs`
  (solo los N más nuevos), `checkpoint_periodic.rs` (disparo periódico y
  rechazo sin config).
- **2PC** (`tests/sink_barrier.rs`): transaccional prepara→commit→valid;
  fallo de capture aborta y descarta; fallo de prepare aborta los ya preparados;
  idempotente se flushea sin prepare; at-least-once no se coordina.
- **Recovery** (`tests/recovery.rs`, `tests/recovery_startup.rs`):
  crash + recovery ≡ sin crash; sin pérdida ni duplicado en la frontera;
  checkpoint ausente = arranque limpio; `latest` corrupto cae a uno anterior;
  retención insuficiente = error explícito.
- **Dynamic views** (`crates/hotlap-engine/tests/dynamic_view.rs`):
  `late_views_match_full_recomputation_and_keep_updating`,
  `late_view_without_retention_is_rejected`,
  `late_view_with_truncated_retention_is_rejected`; e2e SQL en
  `crates/hotlap-sql/tests/dynamic_view.rs`
  (`view_created_after_start_matches_full_recomputation`).

## 10. Verificación

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
