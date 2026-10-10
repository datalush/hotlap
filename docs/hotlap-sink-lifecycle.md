# Ciclo de vida, límites y verificación del sink

Detalle complementario de [sink Hotlap](hotlap-sink.md).

## 1. Ciclo de vida y cierre ordenado

1. En `setup` (antes del primer push) se registran inputs/views y se hace
   `hotlap.tap_view(view)` por cada sink.
2. Tras cada `ingest`, `SinkPump::pump` drena cada vista suscrita y envía los
   deltas a su canal (backpressure).
3. En `shutdown` (o cuando el canal de comandos se cierra), el motor libera
   primero el checkpointer y sus senders, luego cierra el pump y espera las tareas.
   El EOF de los writers **solo autoriza el commit final si el estado global de
   cierre sigue sano**: fallo/cancelación de source, ingest, envío, ACK/writer o
   checkpoint marca el cierre como no limpio y no permite commit implícito. Así,
   un prepare incierto no se convierte en publicación por EOF. El commit final
   tiene un límite de **5 s**; las tareas también tienen espera acotada y se
   abortan/recogen al vencer. El hilo motor corre en un runtime *current-thread*;
   un sink que bloquea sin ceder no puede sondear parada ni timer. `Session` espera
   el hilo con timeout del llamante de **10 s** y, si no responde, lo desacopla y
   devuelve error. Esto limita la espera del caller, no termina a la fuerza hilo
   alguno ni demuestra que trabajo no cooperativo se haya cancelado.

## 2. Fuera de alcance

- `upsert`/`delete` sobre tablas con PK — versión 1 solo **append**.
- **2PC real / exactly-once**: Fluss no ofrece transacciones sink; el runtime
  no promete exactly-once.
- **Varios sinks / fan-out**: versión 1 admite un sink por vista; otro sobre la misma
  vista se **rechaza** con `SqlError::Unsupported` en `CREATE SINK` y, en la
  frontera pública, `Pipeline::validate` lo rechaza antes de abrir writers,
  streams o taps (ver límites de esta versión).
- `CREATE SINK` tras `START`.
- Persistencia de estado / recuperación desde checkpoint.
- **Proyección explícita**: solo `AS SELECT * FROM <mv>`; cualquier otra
  proyección (`SELECT k ...`) se **rechaza** con `SqlError::Unsupported`.

## 3. Límites de la versión 1

- **Un sink por vista.** `take_changes` **drena** el buffer, así que dos sinks
  sobre la misma vista se pisarían; el segundo `CREATE SINK` sobre una vista ya
  suscrita se **rechaza** en la capa SQL (`SqlError::Unsupported`) y
  `Pipeline::validate` lo rechaza antes de abrir writers, streams o taps. Sinks
  sobre vistas **distintas** funcionan; el fan-out no es un objetivo v1.
- **Capacidad de retracción negociada.** `Sink::accepts_retractions` y
  `SinkFactory::accepts_retractions(options)` por defecto son `false`. El
  preflight SQL consulta la capacidad del factory antes de crear writers y luego
  valida el sink construido; `Pipeline::validate` valida sinks existentes. Un
  `GroupAggregate` retractable se rechaza para sinks append-only. `TumbleCount`
  cerrado emite solo `+1` y es compatible con append-only; no se añaden
  deletes/upserts nuevos.
- **`SinkPump::close` y join acotados.** El join de cada task está acotado a 5 s;
  superarlo aborta y recoge la task (reap igualmente acotado) y reporta un error de
  infraestructura en vez de colgar `shutdown`. La cancelación es **cooperativa**:
  aborta una task parkeada en un `await` (por ejemplo un sink detenido), pero no
  puede interrumpir trabajo que nunca cede; en ese caso `close` sigue reportando el
  timeout, no una entrega completada. El hilo del motor se espera con un **timeout
  del llamante (10 s)**; si no responde, se **desacopla** y `shutdown` devuelve
  error: no existe terminación forzada de un hilo de Rust. No se garantiza
  cancelación ni entrega tras un timeout.
- **Desajuste de esquema:** la tabla Fluss destino debe coincidir con el esquema MV;
  de lo contrario, error del writer.
- **Tipos:** solo `Int64`/`Timestamp(ms)`/`Utf8`/`Boolean`; otros se
  rechazan con `Unsupported`.

## 4. Cobertura de pruebas registrada

Cobertura: `hotlap::changes` (`changelog_reconstructs_snapshot`,
`tap_after_first_push_rejected`); `hotlap-connectors::sink_e2e`
(`sink_consolidated_state_matches_snapshot`, `backpressure_loses_no_batch`,
`shutdown_delivers_the_last_changelog`, `commit_runs_after_the_stream_ends`) y
`fluss/sink.rs` unit (`rejects_retraction`, `expands_multiplicity_into_rows`,
`builds_timestamp_column`, `rejects_unsupported_type`); `hotlap-sql::ddl::sink`
(`parses_create_sink`, `rejects_non_star_projection`); `hotlap-sql::sink_e2e`
(wiring completo, sink tras `START` rechazado, vista desconocida rechazada,
segundo sink sobre la misma vista rechazado).
 `fluss_sink_live.rs` es una prueba **ignored** (requiere cluster Fluss activo).
