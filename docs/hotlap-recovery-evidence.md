# Cobertura de pruebas histórica de recuperación

Inventario de pruebas que sustenta el contrato de
[recuperación Hotlap](hotlap-recovery.md). La enumeración es evidencia de cobertura
registrada, no afirmación de una corrida reciente.

## Pruebas registradas

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
  una versión desconocida sigue siendo fatal. `tests/checkpoint_legacy_format.rs`
  fija además que un snapshot v4 real (no sólo la versión mutada) es fatal: ni
  cae al checkpoint anterior ni descarta el commit pendiente.
- **Barrera y entrega** (`tests/sink_barrier.rs`): la intención durable `prepare`
  se escribe antes de llamar al sink; un fallo de preparación/captura aborta y
  borra el marcador solo después de confirmar todos los aborts. Fallo/cancelación
  de rollback conserva el marcador y la incertidumbre. Una vez iniciado `commit`, un
  fallo no aborta participantes porque alguno puede haber confirmado. Los sinks
  `Idempotent` y `AtLeastOnce` esperan `commit`/flush y ACK antes de publicar
  `valid`; la entrega no confirmada no certifica offsets. Los no transaccionales
  no ofrecen rollback. La recuperación solo descarta/replaya si no participa ningún
  sink transaccional; puede haber duplicados.
- **Checkpoint + sink** (`tests/checkpoint_sink.rs`): la barrera drena el canal
  del sink antes de `valid`, de modo que un delta encolado nunca se pierde.
- **Recuperación** (`tests/recovery.rs`, `tests/recovery_startup.rs`):
  crash + recuperación ≡ sin crash; sin pérdida ni duplicado en la frontera;
  checkpoint ausente = arranque limpio; `latest` corrupto cae a uno anterior;
  retención insuficiente = error explícito.
- **Commit re-conducible** (`tests/recovery_commit_marker.rs`,
  `tests/recovery_redrivable.rs`, `tests/recovery_pending_source.rs`,
  `tests/cross_source_pending.rs`, `tests/cross_source_pending_schema.rs`): el
  marker `commit` es durable antes de
  `Sink::commit` y se borra tras `valid`; un commit interrumpido se **promueve**
  (re-conduce commit, sin replay) o se **descarta** con señal explícita
  (warning + `checkpoints_discarded`) solo cuando no participa ningún sink
  transaccional; `commit_redriable` se puede sobreescribir por encima/debajo de la
  capacidad. Si participa cualquier sink transaccional,
  no se permite fallback a replay aunque su commit sea re-conducible: cuerpo
  ilegible o promoción fallida se rechazan porque la transacción pudo confirmarse.
  Solo sin participantes transaccionales se descarta con señal y replaya, con
  posible duplicación. Sin marker, recovery coincide con el válido más nuevo.
- **Identidad de vistas** (`crates/hotlap-runtime/tests/view_identity.rs`,
  `tests/view_identity_resume.rs`, `tests/view_identity_pipeline.rs`,
  `tests/view_identity_session.rs`,
  `crates/hotlap-engine/tests/restore_identity.rs` y la guarda del facade en
  `crates/hotlap/src/engine/tests.rs`): dos vistas con el mismo schema y
  contenido distinto declaradas en orden invertido, un nombre reutilizado con
  otro plan, una vista eliminada (primera o **última**), handles duplicados en el
  snapshot y un `Recovery::resume` con vista renombrada se rechazan
  (`Unsupported`) antes de restaurar el motor o reabrir el source; la sesión SQL
  y el `Pipeline` público lo rechazan antes de abrir un writer/arrancar el pump
  (`factory creates`, reads y commits en cero), el checkpoint sobrevive al
  rechazo para un reintento corregido y el registro completo sigue recuperando
  ambas vistas.
- **Vistas dinámicas** (`crates/hotlap-engine/tests/dynamic_view.rs`):
  `late_views_match_full_recomputation_and_keep_updating`,
  `late_view_without_retention_is_rejected`,
  `late_view_with_truncated_retention_is_rejected`; e2e SQL en
  `crates/hotlap-runtime/tests/sql_dynamic_view.rs` y
  `tests/sql_cross_source_late.rs`
  (`view_created_after_start_matches_full_recomputation`).

## Verificación histórica

Las pruebas enumeradas describen cobertura histórica; los resultados recientes se
registran en el informe de ejecución correspondiente y no se infieren de este documento.
