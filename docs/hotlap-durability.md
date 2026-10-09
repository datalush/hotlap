# Hotlap — durabilidad: checkpoints, recovery, 2PC y vistas dinámicas

- Fecha: 2026-10-08
- Estado: implementado (SP4; commit recuperable en SP8), tests verdes
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
- Persistencia de la historia de inputs para dynamic views
  (`hotlap-recovery.md`).

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
- La creación de directorios sincroniza cada directorio nuevo y el **primer
  ancestro existente** que enlaza el subárbol, para que una entrada recién
  creada sea durable. Con una raíz relativa sin componente existente, ese
  ancestro es el directorio actual (`.`).
- `delete` sincroniza **cada** directorio que pierde una entrada justo después
  del borrado y **antes** de podarlo; si el directorio se poda, su padre queda
  modificado y se sincroniza en el paso siguiente (incluido `root`, que nunca se
  poda). La ausencia se clasifica **sólo** a partir del borrado del fichero: un
  error posterior, aunque sea de tipo `NotFound`/`NotADirectory`, se **propaga**;
  la operación no se declara exitosa.
- `open(root)` crea el directorio raíz (con `fsync` de directorios) si no existe.

> Nota: los tests de orden de `fsync` y de propagación de fallos inyectan el
> fallo sobre un árbol de directorios real (`state/fsio_tests.rs`) o sobre un
> `FsOps` en memoria para rutas relativas (`state/fsio_edge_tests.rs`);
> comprueban el **orden de las escrituras durables**, no el comportamiento de un
> corte eléctrico real.

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
  persistió**: al restaurar, la retención se **invalida**
  (`hotlap-recovery.md`), así que un `build_view` post-start se rechaza aunque la
  retención estuviera activa.

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
- `take(engine, sources)` captura y persiste un checkpoint nuevo (async).

**Formato binario.** El snapshot del motor se codifica con un **frame binario
versionado** (`crates/hotlap-engine/src/core/ipc.rs`); el estado de las fuentes,
como un `SourcesCheckpoint` multifuente en un contenedor `HLSR`
(`runtime/source_checkpoint/`):

- Cabecera fija del frame del engine: magic `HLSP` (4) + versión de frame (4) +
  longitud del payload (8, little-endian), seguido de un payload `bincode`.
- El contenedor de fuentes: magic `HLSR` (4) + versión de layout (4) + el frame
  del engine con el payload `SourcesCheckpoint`.
- El decode **rechaza** magic incorrecto, versión desconocida, longitud que no
  coincide, bytes sobrantes o payloads por encima del límite (`MAX_FRAME_BYTES`),
  devolviendo error en vez de `panic`.
- El `format_version` del `EngineSnapshot` y la versión de layout del contenedor
  son guardas adicionales contra layouts incompatibles.

**Un único formato multifuente.** El mismo contenedor sirve para una o varias
fuentes: cada entrada guarda id, nombre canónico, schema Arrow IPC, lag de
watermark, columna event-time y `SourceState` (offsets por split). No hay
**lectores de formatos anteriores**, migraciones ni fallbacks: un checkpoint
monofuente previo o de versión incompatible produce `Unsupported` (ver
`hotlap-cross-source-joins.md`). La corrupción del **formato actual** sí es
tolerada: recovery cae al predecesor válido más nuevo; una versión incompatible
o un schema que no valida contra las fuentes declaradas es fatal.

**Layout en disco** (namespace bajo `checkpoint/`):

| Clave | Contenido |
| --- | --- |
| `checkpoint/<id>/engine` | snapshot del motor (frame binario) |
| `checkpoint/<id>/sources` | `SourcesCheckpoint` multifuente (contenedor `HLSR`) |
| `checkpoint/<id>/commit` | marcador `1`: intención de commit durable (se borra tras `valid`) |
| `checkpoint/<id>/valid` | marcador `1`: el checkpoint está completo |
| `checkpoint/latest` | id (8 bytes LE) del checkpoint nuevo más reciente |

**Publicación coherente.** Un checkpoint solo es visible cuando **todas** sus
partes están escritas: primero el cuerpo (`engine` + `sources`), luego el
marcador durable `commit`, después el marcador `valid`, y solo entonces el
puntero `latest`. Una escritura interrumpida **nunca** expone un checkpoint
parcial como el actual. El marcador `commit` se borra tras publicar `valid`: un
`commit` presente **sin** `valid` señala a recovery que el proceso cayó en la
ventana de commit (`hotlap-sink-2pc.md`).

**Cobertura del sink.** El pump del motor drena los deltas de cada vista a un
canal acotado que la tarea del sink consume de forma asíncrona. Antes de
preparar y confirmar, la barrera **drena ese canal**: envía una marca de flush
detrás de los lotes encolados y espera la confirmación de la tarea, que responde
solo tras escribirlos. Por tanto, al escribir `valid` **todos** los deltas de
salida hasta ese punto ya llegaron al sink; un checkpoint no puede quedar válido
con salida aún encolada (que un crash no volvería a entregar).

**Retención** (`runtime/retention.rs`): tras publicar, se **podan** los
checkpoints más antiguos para conservar los `retain` más nuevos. El borrado solo
toca claves `checkpoint/<id>/...` (nunca `latest`) y es **idempotente**, así que
un store parcialmente podado se puede podar de nuevo.

**Disparo.** Periódico (config `CheckpointConfig { interval, backend, retain }`)
y **on-demand** (`CHECKPOINT` por el canal de comandos).

## 6. 2PC, recovery y dynamic views

Ver `docs/hotlap-sink-2pc.md`, `docs/hotlap-recovery.md` y
`docs/hotlap-cross-source-joins.md`.

