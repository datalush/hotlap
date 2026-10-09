# Hotlap — recovery, dynamic views y verificación de durabilidad

- Fecha: 2026-10-09
- Estado: implementado (SP4; commit recuperable en SP8), tests verdes
- Alcance: recuperación de checkpoint + replay, vistas materializadas creadas
  tras `START` y cobertura de tests.
- Complementa `docs/hotlap-durability.md` (backend, snapshot, checkpointer) y
  `docs/hotlap-sink-2pc.md` (2PC y ventana de commit).

> Código y comentarios en **inglés**; este documento en español.

## 1. Recovery: checkpoint + replay

`Recovery` (`runtime/recovery.rs`):

1. **Resolver un commit interrumpido** (`Recovery::inspect` / `start`): si hay
   un marker `commit` sin `valid` con el cuerpo completo, C estaba en curso de
   commit; se **promueve** (re-conducir commit + publicar `valid`) si todos los
   sinks son re-conducibles; si ninguno es transaccional, se **descarta C con
   señal explícita** y se replaya desde el anterior; y si algún sink es
   **transaccional** y no re-conducible, se **rechaza** conservando el marcador y
   el cuerpo, porque replayar podría duplicar una transacción confirmada (ver
   `docs/hotlap-sink-2pc.md`). Sin marker, este paso no hace nada. Si publicar
   el commit promovido falla de forma **operativa** (`Storage`), el error
   **propaga** sin leer un fallback, sin descartar el pending y sin reabrir
   ninguna fuente.
2. **Cargar el último checkpoint válido**: `latest` se lee solo para
   clasificarlo, no para elegir. Se escanea el namespace de más nuevo a más
   viejo y se toma el `valid` más nuevo, así un puntero `latest` que se quedó
   atrás (p.ej. su escritura falló) **no** oculta uno ya publicado. Un puntero
   `latest` dañado o un cuerpo del formato actual **corrupto** (truncado, longitud
   incoherente, payload ilegible) se saltan: un tip corrupto no aborta el arranque
   mientras quede un predecesor válido. En cambio, un formato **ajeno o
   incompatible** (magic o versión desconocida, frame interno del motor o
   snapshot del engine con versión no soportada) o un checkpoint que **no valida**
   contra las fuentes declaradas (ids, schema, watermark, renombrado) es
   `Unsupported` **fatal**: no se prueba un lector viejo, no se cae a un
   predecesor y nunca arranca en vacío. Un fallo **operativo** del backend
   (lectura/listado/borrado, `Storage`) también es fatal: se **propaga** sin
   borrar markers, sin retroceder a un predecesor y sin arranque limpio
   silencioso. Sin ningún checkpoint válido (y sin error fatal), es un
   **arranque limpio** (`None`).
3. **Restaurar** el motor con `EngineSnapshot` (`hotlap.restore`).
4. **Reabrir cada source** en los offsets capturados (`Source::resume`) y
   **replayar** desde ahí, alimentando el mismo circuito.

Invariante de replay: `SourceState` guarda el offset del **siguiente** registro a
leer (`records < offset` ya aplicados, `records >= offset` a replayar). Como los
checkpoints se toman **entre polls** del source, reabrir en `offset` **ni pierde
ni duplica** en la frontera.

**Identidad por fuente.** El checkpoint es multifuente (§ `hotlap-durability.md`)
y recovery reanuda **cada** fuente desde su propio offset aplicado: dos fuentes
que usan `SplitId` 0 mantienen mapas de estado separados y no colapsan sus
offsets. Un schema, lag o columna event-time incompatible se rechaza **antes** de
restaurar o consumir.

**Retención explícita.** Si el source ya no puede servir un offset capturado
(p.ej. el log de Fluss podó registros por debajo del offset), `resume` devuelve
un error **explícito**: la recuperación falla ruidosamente en lugar de perder
registros en silencio.

## 2. Dynamic views (retención de inputs)

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
follow-up fuera de SP4.

## 3. Testing

- **`StateBackend` durable** (`crates/hotlap/tests/state_backend.rs`): memoria y
  durable coinciden para la misma secuencia; persistencia a través de reapertura;
  namespace nuevo persistido; `put` atómico/sobrescritura; subdirectorios por
  prefijo; `list` ignora restos no-hex. Errores en
  `hotlap/tests/state_backend_errors.rs`.
- **Checkpoint/restore** (`crates/hotlap-engine/tests/checkpoint.rs`):
  `restore_then_continue_matches_no_restart` (differential). Codec en
  `tests/codec.rs`: round-trip por frame binario, versión desconocida rechazada,
  frames corruptos son errores (no `panic`).
- **Checkpointer** (`hotlap-runtime/tests/`): `checkpoint.rs`
  (`on_demand_checkpoint_is_coherent_and_readable`), `checkpoint_retention.rs`
  (solo los N más nuevos), `checkpoint_periodic.rs` (disparo periódico, rechazo
  sin config y supresión tras fail-stop).
- **Identidad y errores** (`tests/checkpoint_identity.rs`,
  `tests/checkpoint_storage_errors.rs`, `tests/checkpoint_reserve_faults.rs`,
  `tests/checkpoint_recovery_selection.rs`,
  `tests/checkpoint_recovery_storage.rs`): un id reservado no se reutiliza tras
  un intento ambiguo (incluso con el mismo `Checkpointer`), un crash ni una poda,
  y un `Checkpointer` nuevo parte del mayor id presente/reservado; el marker de
  commit se conserva ante una escritura ambigua; la limpieza de un marker obsoleto
  propaga `Storage`; recovery elige el `valid` más nuevo aunque `latest` se quede
  atrás y un fallo de `get`/`list`/publicación se propaga sin descartar ni borrar
  ni reabrir fuentes, mientras que la corrupción del formato actual se tolera y
  una versión desconocida sigue siendo fatal.
- **2PC** (`tests/sink_barrier.rs`): transaccional prepara→commit→valid;
  fallo **operativo** de escritura del cuerpo conserva el marker/cuerpo y no
  aborta los sinks preparados; fallo de prepare aborta los ya preparados;
  fallo de commit aborta el sink que falló y el resto de preparados sin
  confirmar; fallo de flush idempotente aborta los transaccionales preparados;
  idempotente se flushea sin prepare; at-least-once no se coordina.
- **Checkpoint + sink** (`tests/checkpoint_sink.rs`): la barrera drena el canal
  del sink antes de `valid`, de modo que un delta encolado nunca se pierde.
- **Recovery** (`tests/recovery.rs`, `tests/recovery_startup.rs`):
  crash + recovery ≡ sin crash; sin pérdida ni duplicado en la frontera;
  checkpoint ausente = arranque limpio; `latest` corrupto cae a uno anterior;
  retención insuficiente = error explícito.
- **Commit recuperable** (`tests/recovery_commit_marker.rs`,
  `tests/recovery_redrivable.rs`, `tests/recovery_pending_source.rs`,
  `tests/cross_source_pending.rs`, `tests/cross_source_pending_schema.rs`): el
  marker `commit` es durable antes de
  `Sink::commit` y se borra tras `valid`; un commit interrumpido se **promueve**
  (re-conduce commit, sin replay) o se **descarta** con señal explícita
  (warning + `checkpoints_discarded`); `commit_redriable` se puede sobreescribir
  por encima/debajo de la capacidad; sin marker, recovery coincide con el
  válido más nuevo.
- **Dynamic views** (`crates/hotlap-engine/tests/dynamic_view.rs`):
  `late_views_match_full_recomputation_and_keep_updating`,
  `late_view_without_retention_is_rejected`,
  `late_view_with_truncated_retention_is_rejected`; e2e SQL en
  `crates/hotlap-runtime/tests/sql_dynamic_view.rs` y
  `tests/sql_cross_source_late.rs`
  (`view_created_after_start_matches_full_recomputation`).

## 4. Verificación

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
