# Checkpointer: formato, publicación y recuperación

Detalle técnico complementario de [durabilidad Hotlap](hotlap-durability.md).

## Checkpointer: barrera, formato binario y retención

`Checkpointer` (`crates/hotlap-connectors/src/runtime/checkpoint.rs`) escribe
checkpoints **coherentes y versionados** en un `StateBackend`:

- `Checkpointer::new(backend, retain)` fija el almacenamiento y cuántos checkpoints
  conservar (`DEFAULT_RETAIN = 3`, recortado a ≥1).
- `with_sinks(Vec<Arc<SharedSink>>)` añade sinks a la barrera 2PC.
- `resume_after(id)`: continúa secuencia tras checkpoint recuperado, para
  no sobrescribirlo.
- `take(engine, sources)` captura y persiste checkpoint nuevo (asíncrono).

**Formato binario.** El snapshot del motor se codifica con un **frame binario
versionado** (`crates/hotlap-engine/src/core/ipc.rs`); el estado de las fuentes,
como un `SourcesCheckpoint` multifuente en un contenedor `HLSR`
(`runtime/source_checkpoint/`):

- Cabecera fija del frame del motor: magic `HLSP` (4) + versión de frame (4) +
  longitud del payload (8, little-endian), seguida de payload `bincode`.
- Contenedor fuentes: magic `HLSR` (4) + versión de layout (4) + frame del motor
  con payload `SourcesCheckpoint`.
- La decodificación **rechaza** magic incorrecto, versión desconocida, longitud que no
  coincide, bytes sobrantes o payloads por encima del límite (`MAX_FRAME_BYTES`),
  devolviendo error en vez de `panic`.
- `format_version` de `EngineSnapshot` y versión de layout del contenedor
  son guardas adicionales contra layouts incompatibles.

**Un único formato multifuente.** El mismo contenedor sirve para una o varias
fuentes: cada entrada guarda ID, nombre canónico, esquema Arrow IPC, retraso del
watermark, columna de tiempo de evento y `SourceState` (offsets por split).
El contenedor **versión 2** guarda el registro de vistas (`nombre↔handle↔plan`)
para asociar cada nombre con handle y plan, no solo con esquema. No hay lectores
de formatos anteriores ni migraciones: checkpoint monofuente previo o versión
incompatible produce `Unsupported` (ver `hotlap-cross-source-joins.md`). La corrupción del formato actual se tolera: la recuperación cae al predecesor
válido más nuevo; una versión incompatible, un **namespace de vistas que no coincide
exactamente** (nombre, handle o plan
desajustado, o handles duplicados) o un schema que no valida contra las fuentes
declaradas es fatal.

**Estructura en disco** (namespace bajo `checkpoint/`):

| Clave | Contenido |
| --- | --- |
| `checkpoint/<id>/engine` | snapshot del motor (frame binario) |
| `checkpoint/<id>/sources` | `SourcesCheckpoint` multifuente (contenedor `HLSR`) |
| `checkpoint/<id>/prepare` | marcador `1`: intención durable escrita antes de invocar `Sink::prepare` |
| `checkpoint/<id>/commit` | marcador `1`: intención durable escrita antes de invocar `Sink::commit` |
| `checkpoint/<id>/valid` | marcador `1`: el checkpoint está completo |
| `checkpoint/latest` | id (8 bytes LE) del checkpoint nuevo más reciente |
| `checkpoint/reserved` | id (8 bytes LE) más alto reservado nunca reutilizable |

**Publicación coherente.** El orden es reserva → drain → marcador durable
`prepare` → llamadas externas `prepare` → cuerpo (`engine` + `sources`) →
marcador durable `commit` → llamadas `commit` → marcador `valid` → `latest`.
El marcador `prepare` existe antes de cualquier efecto externo de prepare; ante
fallo pre-commit solo se elimina después de que todos los `abort` confirmen éxito.
Un error/cancelación de rollback conserva el marcador. El marcador `commit` se
borra tras publicar `valid`: un `commit` presente **sin** `valid` indica a
recovery una fase de commit incierta (`hotlap-sink-2pc.md`).

**Publicación y limpieza son pasos distintos.** Publicar (`valid` + `latest`) y
podar los antiguos son operaciones separadas: un fallo al escribir `latest` o al
podar **después** de que `valid` se escribió **no** des-publica el checkpoint ni
autoriza a sobrescribir `engine`/`sources` bajo ese id. La poda solo puede
eliminar checkpoints más antiguos que el recién publicado; el puntero `latest`
nunca se poda. El puntero `latest` es **orientativo**: la recuperación no se fía de él
para elegir el checkpoint, sino que escanea el namespace y toma el `valid` más
nuevo, de modo que un `latest` que se quedó atrás no oculta uno ya publicado. Un
fallo **operativo** (`Storage`) al publicar un commit promovido **se propaga** sin
leer alternativa, descartar lo pendiente ni tocar las fuentes.

**Identidad nunca reutilizada.** El id se **reserva** en `checkpoint/reserved`
con la secuencia en memoria **avanzada antes** del `put`, de modo que un intento
ambiguo (un fallo o un crash después de una escritura ya confirmada) no puede
entregar el mismo id a un intento posterior, ni siquiera reintentando desde el
mismo `Checkpointer`. La reserva vive **fuera** de `checkpoint/<id>/`, así que la
poda no la borra. Un `Checkpointer` nuevo parte del mayor id presente **y** del
mayor reservado, y `resume_after` nunca baja de ese suelo; agotar el espacio de
ids falla en vez de envolver a cero.

**Fallo de escritura ambiguo.** Si falla la captura del cuerpo antes de `commit`,
se abortan los sinks preparados y solo tras confirmar **todos** los aborts se
elimina el marcador `prepare`. Un error o cancelación en rollback conserva dicho
marcador y hace fallar el runtime; no se declara rollback seguro. Una vez escrito
el marcador `commit`, los sinks **no** se abortan: un participante puede haber
confirmado, así que se preserva la evidencia para recovery. La limpieza de un
marcador `commit` obsoleto (checkpoint ya `valid`) también propaga un fallo de
borrado como `Storage`.

**Estado explícito y detención segura.** Un intento que falla tras `prepare` deja el
runtime inconsistente: el motor y los offsets de las fuentes **no** se revierten
con los sinks, así que abortar los sinks no restaura el estado. El
`Checkpointer` mantiene estado explícito (`Ready`, `Failed`, `CommitUncertain`) y
**rechaza nuevos intentos** hasta reiniciar; el bucle de servicio deja de sondear
fuentes y de aceptar checkpoints/vistas posteriores a `START`. Solo los fallos
de reserva o previos a `prepare` (que no
descartan ninguna escritura) quedan reintentables. Al reiniciar, recovery
resuelve el marcador pendiente: vuelve a conducir commit y promueve si todos los
sinks permiten esa operación. Si cualquier sink es transaccional, incluso
re-conducible, promoción fallida/cuerpo ilegible conserva evidencia disponible y
causa rechazo sin replay. Solo sin participantes transaccionales se descarta con
señal y se reproduce.

**Cobertura del sink.** El pump del motor drena deltas de cada vista a un
canal acotado que la tarea del sink consume de forma asíncrona. Antes de
preparar y confirmar, la barrera **drena ese canal**: envía una marca de flush
detrás de los lotes encolados y espera la confirmación de la tarea, que responde
solo tras escribirlos. Por tanto, al escribir `valid` **todos** los deltas de
  salida hasta ese punto ya llegaron al sink; un checkpoint no puede quedar válido
  con salida encolada que un crash no volvería a entregar.

**Fronteras de error.** Los fallos de checkpoint se clasifican por **tipo**, no
por el texto del mensaje:

- **Ausencia** (`Missing`): falta el checkpoint o una de sus partes, o falta el
   marcador `valid`. La recuperación puede saltarlo y buscar un predecesor.
- **Corrupción del formato actual** (`Corruption`): los bytes tienen el formato
  vigente pero están dañados (cabecera truncada, longitud incoherente, payload
  ilegible). Es el **único** caso de bytes que la recuperación tolera al caer al
  predecesor válido más nuevo.
- **Incompatibilidad fatal** (`Unsupported`): magic ajeno o versión desconocida
  en el contenedor de fuentes, en el frame interno del motor o en el snapshot.
  Es fatal: no hay lector antiguo, no se cae a un predecesor y nunca arranca en
  vacío.
- **Almacenamiento** (`Storage`): falló una operación de `StateBackend`. Conserva
  el `StateError` subyacente (y su causa de I/O) como `source`. Es **operativo**,
  nunca clasificación de bytes: la recuperación debe propagarlo y **no** puede
  confundirlo con ausencia/corrupción, borrar marcadores ni arrancar en limpio.

Un error de **codificación** (al producir frame desde estado vivo) es interno, no
corrupción persistida ni se clasifica como decodificable.

**Retención** (`runtime/retention.rs`): tras publicar, se **podan** los
checkpoints más antiguos para conservar los `retain` más nuevos. El borrado solo
toca claves `checkpoint/<id>/...` (nunca `latest`) y es **idempotente**, así que
almacenamiento parcialmente podado puede volver a podarse.

**Disparo.** Periódico (configuración `CheckpointConfig { interval, backend, retain }`)
y **bajo demanda** (`CHECKPOINT` por canal de comandos).
