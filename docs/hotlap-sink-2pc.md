# Hotlap — 2PC de sinks y capacidades

- Fecha: 2026-10-09
- Estado: implementado (SP4; commit recuperable en SP8), tests verdes
- Alcance: coordinación 2PC de sinks con capacidades y ventana de commit
  recuperable.
- Complementa `docs/hotlap-durability.md` (backend, snapshot, checkpointer) y
  `docs/hotlap-recovery.md` (recovery, dynamic views, verificación).
- Sink: `docs/hotlap-sink.md`. Diseño:
  `2026-10-08-hotlap-durability-design.md` (local, fuera del repo).

> Código y comentarios en **inglés**; este documento en español.

## 1. Capacidades del sink

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

El sink también **declara** si su `commit` es **re-conducible** tras un
reinicio (`commit_redriable()`, por defecto sólo `Idempotent`): un sink
`Transactional` debe **sobreescribirlo** para optar explícitamente, porque sus
escrituras preparadas pueden no haber sobrevivido al crash y promover un commit
sin confirmar perdería datos en silencio. Un sink puede sobreescribir la
declaración cuando la capacidad subestima o sobreestima la garantía real (p.ej.
un sink aparentemente idempotente cuyos efectos externos no son repetibles).
Recovery usa esta declaración para **promover** o **descartar** la ventana de
crash (ver `docs/hotlap-recovery.md`).

## 2. `SinkBarrier`

`SinkBarrier` (`runtime/sink_barrier.rs`) adapta el protocolo por capacidad:

- **`Transactional`**: `prepare` en la fase uno; `commit` en la fase dos; ante
  cualquier fallo previo a completar el commit, `abort` de los sinks preparados.
- **`Idempotent`**: no hay `prepare`; solo se hace `commit` (flush) en la fase
  dos. Reenviar tras un crash es seguro.
- **`AtLeastOnce`**: no se coordina; sus escrituras ya son visibles.

`SinkBarrier::around(capture)` ejecuta el orden **drain → prepare → capture →
commit**. Si `capture` (snapshot + escritura del cuerpo) falla, o un `commit` /
flush de la fase de confirmación falla, se **abortan todos los sinks preparados
que aún no se hayan confirmado** (incluido el que falló), y el error se propaga:
ningún checkpoint puede llegar a ser válido.

## 3. Protocolo de commit recuperable

El commit de los sinks ocurre **antes** de escribir `valid`; un crash en esa
ventana dejaría el commit hecho pero el checkpoint sin publicar. Para cerrarla,
`take` sigue el orden **drain → prepare → snapshot + escritura del cuerpo →
escribir marker `commit` → commit sinks → `mark_valid` → borrar marker →
retain**:

- el **marker durable** `checkpoint/<id>/commit` se escribe **antes** de
  `Sink::commit` y se borra **después** de publicar `valid`. Si el proceso cae
  después del marker pero antes de `valid`, recovery ve `commit` sin `valid` con
  el cuerpo completo (`engine` + `sources`) y sabe que el checkpoint C estaba
  **en curso de commit**; no re-replaya a ciegas.
- **Promover**: si **todos** los sinks declaran su commit **re-conducible**,
  recovery **re-conduce** `Sink::commit` (idempotente, ya exigido por el
  contrato), publica `valid` y **resume desde C** sin replay.
- **Descartar + señal**: si algún sink no es re-conducible, recovery **borra C**,
  replaya desde el válido anterior y emite una **señal explícita** (warning en el
  canal de errores + métrica `checkpoints_discarded`), nunca en silencio.

**Contrato del sink.** `Sink::commit` **debe tolerar ejecutarse más de una
vez**: la barrera puede confirmar el mismo sink más de una vez y recovery
re-conduce el commit tras un reinicio. Un sink que no pueda repetir su commit
debe declarar `commit_redriable() == false` (p.ej. `AtLeastOnce` y
`Transactional` lo hacen por defecto); recovery entonces descarta C y replaya.

**Garantías por capacidad.**

| Capacidad | Antes (replay a ciegas) | Con el marker |
| --- | --- | --- |
| `Idempotent` | replay (dedup por clave) → effectively-once | promover C sin replay → effectively-once |
| `Transactional` | replay → **duplicado** | re-conducir commit + promover → exactly-once\* (requiere opt-in) |
| `AtLeastOnce` | replay → at-least-once | replay + **señal explícita** → at-least-once |

\* Requiere que `Sink::commit` sea re-conducible tras reinicio (contrato de
arriba): un sink `Transactional` debe declararlo explícitamente con
`commit_redriable() == true`. Si un sink transaccional necesitara un **handle de
transacción durable** para re-conducir su commit, eso es la opción **B** (2PC
real), un follow-up: Fluss no ofrece 2PC y hoy no hay ningún sink `Transactional`.

Sin marker, recovery es **idéntico** al comportamiento anterior (válido más
nuevo, o arranque limpio).

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

## 4. Techo real con Fluss

`fluss/sink.rs`: `FlussSink` elige el writer según la tabla. Tabla con **primary
key** → upsert → `Idempotent`; tabla **log** → append → `AtLeastOnce`. En ambos
modos se **rechazan retracciones** (`diff < 0`). Fluss **no tiene transacción de
sink**: `commit` es el `flush` esperado y `abort` es un no-op intencional (los
appends ya son visibles y los upserts son idempotentes). Por tanto, **con Fluss
el techo es effectively-once (PK) o at-least-once (append)**; exactly-once real
requeriría un sink `Transactional` (hoy ninguno) y 2PC en el almacén.

**Sin 2PC nuevo.** Este slice (joins cross-source) **no** añade una transacción
distribuida ni 2PC nueva: conserva el protocolo SP8 de marker durable, commit del
sink y publicación de `valid`/`latest`, con las mismas garantías por capacidad.
