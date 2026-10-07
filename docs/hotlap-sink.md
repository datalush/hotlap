# Hotlap — Sink: changelog tap y sink Fluss (SINK)

- Fecha: 2026-10-07
- Estado: implementado (SINK), tests verdes
- Alcance: cerrar el primer slice de salida — escribir de forma continua el changelog de una MV a Fluss
- Crates: `hotlap` (kernel, Arrow-free), `hotlap-connectors` (runtime + `FlussSink`), `hotlap-sql` (DDL)
- Plan: SINK (`2026-10-07-hotlap-sink-fluss.md`)
- Diseño: `2026-10-07-hotlap-sink-fluss-design.md`
- Depende de: SP2 (`hotlap-connectors`), SP3 (`hotlap-sql`)

## 1. Propósito

SP1 (kernel), SP2 (conectores) y SP3 (SQL) cubrían el camino de **entrada**
(`Source → kernel → MV consultable`). Este sub-proyecto cierra el primer slice
del lado de **salida**: exponer el **changelog** (los deltas) de una vista
materializada como un stream continuo y escribirlo a **Fluss** en modo
**append-only**.

Hay tres capas nuevas, deliberadamente separadas:

1. **Kernel** (`crates/hotlap`): primitiva *pull* `take_changes(view)` — pura,
   Arrow-free.
2. **Runtime** (`hotlap-connectors`): `ChangelogStream` sobre un canal acotado
   con **backpressure**, y el trait `Sink` **async**.
3. **Sink Fluss** (`hotlap-connectors`): `FlussSink` append-only.

La capa **SQL** (`hotlap-sql`) expone la DDL `CREATE SINK ... AS SELECT * FROM mv`
y conecta el tap en `START`.

## 2. Arquitectura (3 capas)

```
kernel (crates/hotlap)        runtime (hotlap-connectors)         sink (hotlap-connectors)
take_changes(view) ─drena──▶  ChangelogStream (mpsc acotado) ──▶  Sink::write(stream) ──▶ FlussSink
 (buffer en inspect)          backpressure: pausa el source        (task por sink)
```

El **kernel** solo calcula deltas; el **runtime** es dueño de la entrega
(buffer/backpressure/fan-out); el **sink** consume el stream. No se mete política
de entrega en el kernel.

## 3. Tap del kernel

Dos métodos nuevos en `Hotlap` (y en el trait `IncrementalCore`):

| Método | Firma | Semántica |
| --- | --- | --- |
| `tap_view` | `(&mut self, name: &str) -> Result<(), HotlapError>` | Suscribe la vista a su changelog. **Solo antes del primer push** (pre-freeze), como `build_view`. |
| `take_changes` | `(&mut self, name: &str) -> Result<Vec<(Row, i64)>, HotlapError>` | **Drena** los deltas `(row, diff)` acumulados desde el último drenado. `[]` si la vista no está suscrita. |

- El buffer es **por vista** (`ViewState::changes`) y se llena en el `inspect`
  existente del dataflow (`build.rs`), que ya observa cada `(row, _time, diff)`.
- **Solo** las vistas suscritas con `tap_view` reciben buffer (`tapped:
  HashSet<ViewId>`, fase `Building`); una vista no suscrita **no acumula**.
- `take_changes` hace `std::mem::take`: devuelve el Z-set **sin consolidar** del
  último push y deja el buffer vacío. El consumidor consolida si lo desea.
- La suscripción se congela con el dataflow en el primer push: no se puede
  `tap_view` una vista ya en ejecución.

Propiedad diferencial: consolidar la secuencia de changelogs reconstruye el
`snapshot` de la vista (test `changes.rs`).

## 4. Runtime: `ChangelogStream` y backpressure

En `hotlap-connectors/src/runtime/sink.rs`:

- Por sink, un canal **`tokio::sync::mpsc` acotado** (`CHANNEL_CAPACITY = 64`).
- `ChangelogStream` es el extremo receptor adaptado a `futures::Stream`
  (`Item = Result<ChangeBatch, ConnectorError>`); es lo que recibe `Sink::write`.
- Un **task** por sink ejecuta `sink.write(ChangelogStream).await` hasta el cierre
  del canal y, al terminar, `commit` (o `abort` si `write` falló).
- `SinkPump::pump` corre **tras cada `ingest`**: por cada vista suscrita toma sus
  deltas y hace `sender.send(...).await`. Si el canal está lleno, el `await`
  **bloquea el bucle del engine**, que pausa la lectura del source: un sink lento
  **ralentiza la ingesta, no pierde datos**. Un buffer vacío no envía nada.

## 5. Trait `Sink` async

`hotlap-connectors/src/sink.rs`. El trait de SP2 (síncrono, solo forma) pasa a
**async** con `#[async_trait]`:

```rust
pub type ChangeStream =
    Pin<Box<dyn Stream<Item = Result<ChangeBatch, ConnectorError>> + Send>>;

#[async_trait]
pub trait Sink: Send + Sync {
    async fn write(&self, changes: ChangeStream) -> Result<(), ConnectorError>;
    async fn commit(&self) -> Result<(), ConnectorError>;
    async fn abort(&self) -> Result<(), ConnectorError>;
}
```

- `write` consume el stream hasta que termina (cierre del canal) y vuelve.
- `commit` = confirmar lo escrito desde el último commit.
- `abort` = descartar lo escrito desde el último commit.

El task del runtime dirige el ciclo de vida completo: `write` hasta el cierre del
canal y, al terminar, `commit` (que entrega lo fire-and-forget del sink Fluss) o
`abort` si `write` falló. La forma sigue siendo nominal (sin transacción ni
exactly-once), pero el runtime ya invoca el contrato, no solo el test live.

## 6. `FlussSink` (append-only)

`hotlap-connectors/src/fluss/sink.rs`.

- `FlussSink::open_from_bootstrap(bootstrap, path, schema)` conecta a Fluss
  (`Config`), resuelve la tabla `<database>/<table>` con `parse_path` y abre un
  `AppendWriter` (`table.new_append().create_writer()`).
- `write`: por cada `ChangeBatch`, convierte filas del kernel → `RecordBatch`
  Arrow con el **schema de la MV** (`sink_convert::rows_to_batch`) y llama
  `append_arrow_batch`. Los batches sin filas se saltan.
- **Rechazo de retracciones:** `retraction_check` exige `diff >= 0`; cualquier
  `diff < 0` devuelve `ConnectorError::Unsupported` (una tabla log append-only no
  borra). Se invoca dentro de `rows_to_batch`, por lo que `write` lo aplica.
- `commit` → `writer.flush().await`; los errores de `append` diferidos (fire and
  forget) afloran aquí.
- `abort` → `Ok(())`: el append no tiene transacción; lo ya encolado es visible.

Conversión de tipos (`sink_convert.rs`): `diff` es la **multiplicidad** (cada
fila se repite `diff` veces; `diff = 0` se descarta); se soportan `Int64`,
`Timestamp(ms)`, `Utf8` y `Boolean` (`null → Null`); cualquier otro tipo Arrow es
`Unsupported`. Los `Timestamp` se construyen como `Int64` epoch-ms y se castean al
tipo declarado.

## 7. SQL: `CREATE SINK`

`hotlap-sql` reconoce la gramática mínima (parser de strings, como el resto de
DDL):

```sql
CREATE SINK <nombre>
  WITH (connector='fluss', bootstrap='<addr>', table='<db>/<tabla>')
  AS SELECT * FROM <mv>;

START;
```

- `ddl::CreateSink { name, options, view }`; `view` es el nombre tras `FROM`.
- `create_sink` valida que la vista exista y que el nombre del sink no esté
  repetido; registra el `SinkDef` en el catálogo. **No** abre el connector aquí.
- La apertura ocurre en `START`: `build_sinks` resuelve el **schema de la MV** y
  pide al `SinkFactory` que construya el sink. Así el factory puede ser async y
  el schema ya está validado.
- `SinkFactory` (async) abstrae la creación; el default es `FlussSinkFactory`,
  que exige `connector='fluss'`, `bootstrap` y `table`. `SqlSession::open_with_factories`
  inyecta factories fake en tests.
- **Ventana DDL:** `CREATE SINK` después de `START` → `SqlError::Unsupported`
  (`reject_after_start`), igual que source/MV.

## 8. Ciclo de vida y cierre ordenado

1. En `setup` (antes del primer push) se registran inputs/views y se hace
   `hotlap.tap_view(view)` por cada sink.
2. Tras cada `ingest`, `SinkPump::pump` drena cada vista suscrita y envía los
   deltas a su canal (backpressure).
3. En `shutdown` (o cuando el canal de comandos se cierra), `SinkPump::close`
   **suelta los senders** (cierra los canales), **espera** a cada task y devuelve
   el primer error. Soltar el sender termina el changelog, de modo que el **último
   lote se entrega** antes de que el task termine; al aceptar el stream, el task
   llama a `commit` (o a `abort` si `write` falló). El join de cada task está
   acotado por un **timeout** de 5 s para que un sink colgado no bloquee
   `shutdown` indefinidamente; el timeout se reporta como error del sink.

## 9. No-goals

- `upsert`/`delete` sobre tablas con PK — v1 solo **append**.
- **2PC real / exactly-once**: `commit`/`abort` son forma nominal (SP4).
- **N sinks / fan-out**: v1 un sink por vista; un segundo `CREATE SINK` sobre la
  misma vista se **rechaza** con `SqlError::Unsupported` (ver §10).
- `CREATE SINK` tras `START`.
- Persistencia de estado / recuperación desde checkpoint.
- **Proyección explícita**: solo `AS SELECT * FROM <mv>`; cualquier otra
  proyección (`SELECT k ...`) se **rechaza** con `SqlError::Unsupported`.

## 10. Límites y residuos v1

- **`commit` cableado al cierre del stream.** El task del sink llama a `commit`
  al aceptar el changelog (o a `abort` si `write` falló), de modo que el `flush`
  de Fluss ya no depende solo del cierre del writer. Falta el **commit
  periódico** por ciclo y la semántica 2PC real (SP4).
- **Un sink por vista.** `take_changes` **drena** el buffer, así que dos sinks
  sobre la misma vista se pisarían; el segundo `CREATE SINK` sobre una vista ya
  suscrita se **rechaza** en la capa SQL (`SqlError::Unsupported`). Sinks sobre
  vistas **distintas** funcionan; el fan-out no es un objetivo v1.
- **`SinkPump::close` con timeout.** El join de cada task está acotado a 5 s;
  superarlo reporta un error de infraestructura en vez de colgar `shutdown`.
- **Parkeado (LOW) — sin drenado final antes de `close`.** `close` no hace un
  último `pump` defensivo: se confía en que el bucle del engine drena tras cada
  `ingest`. Un `close` sin drenado previo podría perder los deltas pendientes;
  queda como endurecimiento defensivo si el orden cambia.
- **Parkeado (LOW) — materialización de multiplicidad en el hilo del engine.**
  `rows_to_batch` expande `diff` repitiendo filas en el hilo del engine (no hay
  límite de `diff`), por lo que un multiplicidad grande consume CPU/memoria del
  engine. Se acepta mientras la ventana v1 no genere multiplicidades grandes.
- **Schema mismatch:** la tabla Fluss destino debe coincidir con el schema de la
  MV; si no, el error aflora en el writer.
- **Tipos:** solo `Int64`/`Timestamp(ms)`/`Utf8`/`Boolean`; otros son
  `Unsupported`.

## 11. Verificación

```bash
cargo fmt --all -- --check
cargo clippy -p hotlap -p hotlap-connectors -p hotlap-sql --all-targets -- -D warnings
cargo test -p hotlap
cargo test -p hotlap-connectors
cargo test -p hotlap-sql
```

Cobertura: `hotlap::changes` (`changelog_reconstructs_snapshot`,
`tap_after_first_push_rejected`); `hotlap-connectors::sink_e2e`
(`sink_consolidated_state_matches_snapshot`, `backpressure_loses_no_batch`,
`shutdown_delivers_the_last_changelog`, `commit_runs_after_the_stream_ends`) y
`fluss/sink.rs` unit (`rejects_retraction`, `expands_multiplicity_into_rows`,
`builds_timestamp_column`, `rejects_unsupported_type`); `hotlap-sql::ddl::sink`
(`parses_create_sink`, `rejects_non_star_projection`); `hotlap-sql::sink_e2e`
(wiring completo, sink tras `START` rechazado, vista desconocida rechazada,
segundo sink sobre la misma vista rechazado).
`fluss_sink_live.rs` es un test **ignored** (requiere un cluster Fluss vivo).
