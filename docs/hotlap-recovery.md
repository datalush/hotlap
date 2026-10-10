# Hotlap — recuperación, vistas dinámicas y durabilidad

- Fecha: 2026-10-09
- Estado histórico: implementación y pruebas registradas al 2026-10-09.
- Alcance: recuperación de checkpoint + replay, vistas materializadas creadas
  tras `START` y cobertura de tests.
- Complementa `docs/hotlap-durability.md` (backend, snapshot, checkpointer) y
  `docs/hotlap-sink-2pc.md` (2PC y ventana de commit).

> El documento está en español; el código y los comentarios del código, en inglés.

## 1. Recuperación: checkpoint y replay

`Recovery` (`runtime/recovery.rs`):

1. **Resolver un commit interrumpido** (`Recovery::inspect` / `start`): si hay
   un marcador `commit` sin `valid` con el cuerpo completo, C estaba en curso de
   commit; se **promueve** (re-conducir commit + publicar `valid`) si todos los
    sinks son re-conducibles; si alguno no lo es y todos los sinks son no
    transaccionales, se **descarta C con señal explícita** y se replaya desde el
    anterior (un sink al menos una vez puede duplicar). Si participa **cualquier
    sink transaccional**, se **rechaza** conservando marcador/cuerpo disponible
    porque podría duplicarse una transacción confirmada (ver `hotlap-sink-2pc.md`).
    Esto también rige si es re-conducible: cuerpo pendiente **ilegible** o fallo
    de promoción no autoriza fallback/replay. Solo si no hay participantes
    transaccionales puede descartarse y replayarse tras fallo no operativo de
    re-conducción. Sin marker, este paso no hace nada. Si publicar el commit promovido
   falla de forma **operativa** (`Storage`), el error **se propaga** sin fallback,
   descartar lo pendiente ni reabrir fuentes.
2. **Cargar el último checkpoint válido**: `latest` se lee solo para
   clasificarlo, no para elegir. Se escanea el namespace de más nuevo a más
   viejo y se toma el `valid` más nuevo, así un puntero `latest` que se quedó
   atrás (p.ej. su escritura falló) **no** oculta uno ya publicado. Un puntero
    `latest` dañado o un cuerpo del formato actual **corrupto** (truncado, longitud
    incoherente, payload ilegible) o **ausente** se saltan como candidatos: un tip
    incompleto no aborta el arranque mientras quede un predecesor válido. En cambio, un formato **ajeno o
   incompatible** (magic o versión desconocida, frame interno del motor o
   snapshot del engine con versión no soportada) o un checkpoint que **no valida**
   contra las fuentes declaradas (ids, schema, watermark, renombrado) es
   `Unsupported` **fatal**: no se prueba un lector viejo, no se cae a un
   predecesor y nunca arranca en vacío. La versión del snapshot se lee del
   prefijo del cuerpo **antes** de decodificar el layout, así que un snapshot de
   una versión anterior —cuyo layout ya no decodifica— también es `Unsupported`,
   no corrupción del formato actual. Un fallo **operativo** del backend
   (lectura/listado/borrado, `Storage`) también es fatal: se **propaga** sin
   borrar markers, sin retroceder a un predecesor y sin arranque limpio
   silencioso. Sin ningún checkpoint válido (y sin error fatal), es un
   **arranque limpio** (`None`).
3. **Restaurar** el motor con `EngineSnapshot` (`hotlap.restore`).
4. **Reabrir cada fuente** en offsets capturados (`Source::resume`) y
   **reproducir** desde allí, alimentando el mismo circuito.

Invariante de replay: `SourceState` guarda el offset del **siguiente** registro a
leer (`records < offset` ya aplicados, `records >= offset` a replayar). Como los
checkpoints se toman **entre polls** del source, reabrir en `offset` **ni pierde
ni duplica** en la frontera.

**Identidad física por fuente y sink.** El checkpoint es multifuente (ver
`hotlap-durability.md`) y la recuperación reanuda **cada** fuente desde su propio
offset aplicado: dos fuentes que usan `SplitId` 0 mantienen mapas separados. Cada
fuente persistida también lleva su identidad física, distinta del nombre SQL,
schema y splits; un dataset reemplazado se rechaza antes de `Source::resume`. Un
schema, lag o columna event-time incompatible también se rechaza **antes** de
restaurar o consumir.

Cada sink durable queda enlazado por `(binding_name, view)` a su destino físico y
metadatos de entrega (capability, retracts y `commit_redriable`). Añadir, quitar,
renombrar o retargetear un sink —o cambiar esos metadatos— no equivale al set
capturado: recovery lo rechaza antes de abrir fuentes/writers, preparar, confirmar
o borrar evidencia. El manifiesto `HLPM` separado se escribe antes de `prepare`;
si falta, está corrupto o no concuerda con el cuerpo, es fatal y no autoriza
fallback silencioso. Un pipeline durable requiere identidades explícitas y los
sinks usan `SinkSpec::named` / `SinkSync::sink_only_named`.

**Identidad de vistas.** El snapshot del motor guarda los planes por *handle*
numérico, pero **no** el nombre declarado. Para que un restart no reasigne un
handle a otra vista con el mismo schema (p. ej. el orden de `CREATE MATERIALIZED
VIEW` invertido, o el mismo nombre con otro plan), el checkpoint persiste un
registro `nombre↔handle↔plan` (contenedor `HLSR` versión 3, sin lector legacy).
Al recuperar se exige que el **namespace completo** coincida: registro,
snapshot del motor y declaración deben nombrar exactamente las mismas vistas, con
el mismo número, nombres, handles y planes (nada de "restaurar pero no
rebindear"). Una vista creada antes de `START` que faltó en el registro, o una
vista late guardada que no se redeclara **idéntica**, se rechaza; así un nombre
nunca se pierde en silencio. Los handles duplicados en el snapshot del motor se
rechazan **antes** de insertar, para no validar un plan y restaurar otro. Un
desajuste es `Unsupported` **antes** de restaurar el motor, re-conducir un commit
o abrir cualquier writer, y `Recovery::resume` vuelve a validar la identidad por
su cuenta (no confía en el caller). La sesión SQL valida el mismo registro contra
las vistas compiladas **antes** de que el `SinkFactory` abra un writer, y el
`Pipeline` público lo valida **antes** de arrancar el pump; la sesión también
compara fuentes y vistas contra la misma elección de recovery antes de abrir
sinks o iniciar lecturas. Para un `START` durable,
`SinkFactory::describe(binding_name, options, schema, view)` debe resolver el
destino y sus metadatos sin crear writer. `None` rechaza antes de `create`; tras
crear, el runtime vuelve a verificar identidad y metadatos del sink abierto. Para
promover un commit pendiente, el factory debe declarar
`may_redrive_commit(options)` y el sink abierto debe confirmar esa capacidad; el
valor por defecto es `false` y Fluss no declara re-drive durable.
Si el re-drive falla y se considera replay, el fallback valida identidad de
fuentes y vistas **antes** de descartar el cuerpo pendiente; un fallback
incompatible conserva toda la evidencia durable.
El commit EOF requiere
además un cierre global sano, nunca se infiere solo de EOF. Así un rechazo deja
`factory creates`, reads y commits en cero. En SQL, el preflight consulta
`SinkFactory::accepts_retractions(options)` antes de abrir writers y revalida la
capacidad del sink construido. El orden de
declaración de las **fuentes** sigue siendo libre (ids canónicos y ordenados);
solo el orden de las **vistas** cambia los handles y puede rechazarse
explícitamente.

**Retención explícita.** Si el source ya no puede servir un offset capturado
(p.ej. el log de Fluss podó registros por debajo del offset), `resume` devuelve
un error **explícito**: la recuperación falla ruidosamente en lugar de perder
registros en silencio.

## 2. Vistas dinámicas (retención de entradas)

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
`restore` **invalida** la retención (`hotlap-durability.md`). La invalidación es
**permanente** (marca el log como truncado y lo vacía, y no se revierte al seguir
registrando deltas nuevos): tras un recovery, el estado restaurado es correcto
pero un `build_view` post-start se rechaza **para siempre** en esa sesión. La
persistencia de la historia de inputs (o el replay desde el source) es un
follow-up fuera de alcance.

## 3. Cobertura de pruebas

La matriz histórica de pruebas de backend, checkpoint, recuperación y vistas se
conserva en [evidencia de recuperación](hotlap-recovery-evidence.md).
