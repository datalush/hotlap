# Hotlap — 2PC de sinks y capacidades

- Fecha: 2026-10-09
- Estado: implementado, tests verdes
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
reinicio (`commit_redriable()`, por defecto `false`). La idempotencia de replay
**no** implica que un commit interrumpido pueda completarse: un sink que sólo
encola escrituras en memoria (como Fluss) pierde la cola al morir el proceso, de
modo que una instancia nueva no puede entregar lo que aceptó la anterior. Un
sink **sobreescribe** la declaración a `true` sólo cuando mantiene estado
preparado durable o su commit re-conducido es realmente un no-op, y su `commit`
tolera ejecutarse más de una vez. Recovery usa esta declaración para **promover**
o **descartar** la ventana de crash (ver `docs/hotlap-recovery.md`).

El sink también **negocia retracciones**: `accepts_retractions()` por defecto es
`false` (append-only). El runtime **rechaza antes de arrancar/escribir** un plan
que pueda retractar (por ejemplo, un agregado por clave) si el sink no declara
soporte; sólo un sink que de verdad aplica diffs negativos debe sobreescribirlo
a `true`. Una ventana tumbling incorpora deltas mientras está abierta, pero al
cerrar emite su resultado final una sola vez con diff positivo, por lo que esa
agregación final es compatible con un sink append-only.

La negociación de la capa SQL ocurre en dos pasos. `SinkFactory::accepts_retractions(options)`
(por defecto `false`) se comprueba **antes** de `create`, de modo que un plan
retractor se rechaza sin abrir ningún writer; el sink creado se vuelve a validar
con `Sink::accepts_retractions`, así que un factory no puede quedarse corto. El
mismo preflight rechaza dos sinks de una vista antes de abrir ninguno. `START`
sólo consume la config de checkpoint tras superar todas las comprobaciones, de
modo que un pipeline rechazado no pierde la durabilidad que un reintento
necesita.

## 2. `SinkBarrier`

`SinkBarrier` (`runtime/sink_barrier.rs`) adapta el protocolo por capacidad:

- **`Transactional`**: `prepare` en la fase uno; `commit` en la fase dos; ante
  cualquier fallo previo a completar el commit, `abort` de los sinks preparados.
- **`Idempotent`**: no hay `prepare`; solo se hace `commit` (flush) en la fase
  dos. Reenviar tras un crash es seguro.
- **`AtLeastOnce`**: no hay `prepare`, pero su `commit` (flush/ACK) se espera
  **antes** de publicar `valid`: un append puede ser visible pero no confirmado,
  y una entrega pendiente o un flush fallido no puede certificar los offsets.

`SinkBarrier` ejecuta el orden **drain → prepare → capture → commit**. Un fallo
**antes** de la fase de commit (en `prepare`, o en `capture` sin cuerpo durable)
aborta los sinks ya preparados, de modo que ninguno quede con una transacción a
medio abrir; el marcador `commit` se borra **antes** de abortar y, si ese borrado
falla, no se aborta nada y el error se propaga como `Storage`. Un fallo
**durante** la fase de commit **no aborta nada**: un participante puede haber
confirmado ya, y revertirlo sería incorrecto. En ese caso se conservan el
marcador y el cuerpo durables para que recovery re-conduzca (si el sink es
reconducible) o descarte y replaye. El error se propaga en todos los casos, de
modo que ningún checkpoint queda válido por accidente.

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
- **Descartar + señal**: si **ningún** sink es transaccional y alguno no es
  re-conducible, recovery **borra C**, replaya desde el válido anterior y emite
  una **señal explícita** (warning en el canal de errores + métrica
  `checkpoints_discarded`), nunca en silencio. Un sink idempotente deduplica el
  replay; uno at-least-once documenta la posible duplicación.
- **Rechazar sin replay**: si un sink **transaccional** no es re-conducible,
  replayar podría duplicar una transacción que ya está confirmada y no hay forma
  de deshacerla. Recovery **no descarta**: conserva el marcador y el cuerpo,
  falla con un error explícito y deja la resolución a un operador. El rechazo
  también aplica si el cuerpo pendiente **no se puede decodificar o reconstruir**
  (no hay nada que promover y replay sería inseguro) y si la **re-conducción**
  falla con un error no operativo. Esto no añade un handle de transacción durable
  ni 2PC nuevo a Fluss.

**Contrato del sink.** `Sink::commit` **debe tolerar ejecutarse más de una
vez**: la barrera puede confirmar el mismo sink más de una vez y recovery
re-conduce el commit tras un reinicio. Un sink que no pueda repetir su commit
**o** que sólo mantenga estado volátil en memoria debe dejar
`commit_redriable() == false` (el valor por defecto, para cualquier capacidad);
recovery entonces descarta C y replaya.

**Garantías por capacidad.**

| Capacidad | Antes (replay a ciegas) | Con el marker |
| --- | --- | --- |
| `Idempotent` | replay (dedup por clave) → effectively-once | replay → effectively-once (no re-conduce salvo opt-in) |
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
- En la fase de commit se **flushean antes los sinks no reversibles**
  (`Idempotent` y `AtLeastOnce`) y luego se confirman los transaccionales
  preparados. Si un no reversible fallara después de que un transaccional ya
  confirmó, el replay duplicaría; flush primero evita esa ventana.
- Un sink transaccional puede ser confirmado por la barrera **y** al cerrar el
  canal; `Sink::commit` debe tolerar ejecutarse **más de una vez**.

## 4. Techo real con Fluss

`fluss/sink.rs`: `FlussSink` elige el writer según la tabla. Tabla con **primary
key** → upsert → `Idempotent`; tabla **log** → append → `AtLeastOnce`. En ambos
modos se **rechazan retracciones** (`diff < 0`): `accepts_retractions()` es
`false`, así que un plan retractor se rechaza antes de escribir. Fluss **no tiene
transacción de sink**: `commit` es el `flush` esperado y `abort` es un no-op
intencional (los appends ya son visibles y los upserts son idempotentes). Como su
writer sólo encola en memoria, Fluss **no** declara su commit re-conducible: un
reinicio replaya en vez de re-conducir. Por tanto, **con Fluss el techo es
effectively-once (PK) o at-least-once (append)**; exactly-once real requeriría un
sink `Transactional` (hoy ninguno) y 2PC en el almacén.

**Sin 2PC nuevo.** Estas correcciones **no** añaden una transacción distribuida ni
2PC nueva: conservan el protocolo de marker durable, commit del sink y
publicación de `valid`/`latest`, con las mismas garantías por capacidad.
