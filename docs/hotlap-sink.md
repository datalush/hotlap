# Hotlap — Sink: changelog tap y sink Fluss

- Fecha: 2026-10-07
- Estado histórico: implementación y pruebas registradas al 2026-10-07.
- Alcance: cerrar el primer slice de salida — escribir de forma continua el changelog de una MV a Fluss
- Crates: `hotlap` (fachada del kernel columnar), `hotlap-connectors` (runtime + `FlussSink`), `hotlap-sql` (DDL)

## 1. Propósito

El camino de **entrada** (`Source → kernel → MV consultable`) ya estaba cubierto
por el kernel, los conectores y la capa SQL. Este primer incremento completa la
salida: exponer el **changelog** (los deltas) de una vista
materializada como un stream continuo y escribirlo a **Fluss** en modo
**append-only**.

Hay tres capas diferenciadas:

1. **Kernel** (`crates/hotlap`): primitiva de extracción `take_changes(view)` — pura,
   Arrow-nativa.
2. **Runtime** (`hotlap-connectors`): `ChangelogStream` sobre un canal acotado
   con **backpressure**, y el trait `Sink` **async**.
3. **Sink Fluss** (`hotlap-connectors`): `FlussSink` de solo append.

La capa **SQL** (`hotlap-sql`) expone la DDL `CREATE SINK ... AS SELECT * FROM mv`
y conecta el tap en `START`.

## 2. Arquitectura (tres capas)

```
kernel (crates/hotlap)        runtime (hotlap-connectors)         sink (hotlap-connectors)
take_changes(view) ─drena──▶  ChangelogStream (mpsc acotado) ──▶  Sink::write(stream) ──▶ FlussSink
 (buffer en inspect)          backpressure: pausa el source        (task por sink)
```

El **kernel** solo calcula deltas; el **runtime** gestiona la entrega
(buffer/contrapresión/distribución); el **sink** consume el stream. No se añade política
de entrega en el kernel.

## 3. Captura del changelog en el kernel

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

## 4. Runtime: `ChangelogStream` y contrapresión

En `hotlap-connectors/src/runtime/sink.rs`:

- Por sink, un canal **`tokio::sync::mpsc` acotado** (`CHANNEL_CAPACITY = 64`).
- `ChangelogStream` es el extremo receptor adaptado a `futures::Stream`
  (`Item = Result<ChangeBatch, ConnectorError>`); es lo que recibe `Sink::write`.
- Una **tarea** por sink ejecuta `sink.write(ChangelogStream).await` hasta cerrar
  el canal; el cierre coordinado decide si se permite el commit EOF.
- `SinkPump::pump` corre **tras cada `ingest`**: por cada vista suscrita toma sus
  deltas y hace `sender.send(...).await`. Si el canal está lleno, el `await`
  **bloquea el bucle del engine**, que pausa la lectura del source: un sink lento
  **ralentiza la ingesta, no pierde datos**. Un buffer vacío no envía nada.

## 5. Trait `Sink` asíncrono

`hotlap-connectors/src/sink.rs`. El trait, inicialmente síncrono y solo forma,
pasa a **async** con `#[async_trait]`:

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
- `accepts_retractions` indica si el sink aplica deltas negativos; por defecto
  `false` (solo append). El runtime rechaza planes retractores antes de escribir.
- `commit_redriable` = si un commit interrumpido puede re-conducirse tras un
  reinicio; por defecto `false`. Una cola volátil en memoria no habilita el
  re-drive aunque el replay sea idempotente.

La tarea consume `write` hasta el EOF del canal. El commit final (que espera el
flush/ACK del writer Fluss) solo se intenta si **todo el cierre global** terminó
sin fallo ni cancelación; EOF por sí solo no es autorización. Un error de source,
push, writer/ACK o checkpoint hace inseguro el EOF y se propaga en el cierre.
Este commit final está acotado a 5 s. El runtime invoca el contrato en ejecución,
sin añadir transacción ni exactly-once.

## 6. `FlussSink` (solo append)

`hotlap-connectors/src/fluss/sink.rs`.

- `FlussSink::open_from_bootstrap(bootstrap, path, schema)` conecta a Fluss
  (`Config`), resuelve la tabla `<database>/<table>` con `parse_path` y abre un
  `AppendWriter` (`table.new_append().create_writer()`).
- `write`: por cada `ChangeBatch`, convierte filas del kernel → `RecordBatch`
  Arrow con el **esquema de la MV** (`sink_convert::rows_to_batch`) y llama a
  `append_arrow_batch`. Se omiten lotes sin filas.
- **Rechazo de retracciones:** `accepts_retractions()` es `false`, así que el
  runtime rechaza antes de arrancar/escribir un plan que pueda retraer; además
  `retraction_check` exige `diff >= 0` en cada lote (defensa residual): cualquier
  `diff < 0` devuelve `ConnectorError::Unsupported` (una tabla log de solo append no
  borra). Se invoca dentro de `rows_to_batch`, por lo que `write` lo aplica.
- `commit` → `writer.flush().await`; errores de `append` diferidos (fire-and-forget)
  aparecen aquí. Fluss **no** declara el commit re-conducible: su writer
  encola en memoria y una instancia nueva tras un crash no puede entregar lo
  perdido, así que la recuperación reproduce en vez de promover.
- `abort` → `Ok(())`: el append no tiene transacción; lo ya encolado es visible.

Conversión de tipos (`sink_convert.rs`): `diff` es la **multiplicidad** (cada
fila se repite `diff` veces; `diff = 0` se descarta); se soportan `Int64`,
  `Timestamp(ms)`, `Utf8` y `Boolean` (`null → Null`); otro tipo Arrow produce
  `Unsupported`. `Timestamp` se construye como `Int64` epoch-ms y se convierte al
  tipo declarado.

## 7. SQL: `CREATE SINK`

`hotlap-sql` reconoce la gramática mínima (parser de cadenas, como el resto de
DDL):

```sql
CREATE SINK <nombre>
  WITH (connector='fluss', bootstrap='<addr>', table='<db>/<tabla>')
  AS SELECT * FROM <mv>;

START;
```

- `ddl::CreateSink { name, options, view }`; `view` es el nombre tras `FROM`.
- `create_sink` valida que la vista exista y que el nombre sink no esté
  repetido; registra el `SinkDef` en el catálogo. **No** abre el connector aquí.
- La apertura ocurre en `START`: tras una validación preliminar que rechaza vistas
  repetidas y planes retractores para sinks sin soporte, `build_sinks` resuelve el
  **schema de la MV** y pide al `SinkFactory` que construya el sink. Así el factory
  ser asíncrono y el esquema ya está validado.
- `SinkFactory` asíncrono abstrae creación; el predeterminado es `FlussSinkFactory`,
  que exige `connector='fluss'`, `bootstrap` y `table`. `SqlSession::open_with_factories`
  inyecta factories de prueba.
- `SinkFactory::accepts_retractions(options)` (por defecto `false`) se comprueba
  antes de `create`, de modo que un plan retractor o una vista repetida se
  rechazan sin abrir writers; el sink creado se revalida para que factory
  no pueda quedarse corto. `START` sólo consume la config de checkpoint tras pasar
  validación preliminar, así un rechazo no pierde durabilidad.
- **Ventana DDL:** `CREATE SINK` tras `START` → `SqlError::Unsupported`
  (`reject_after_start`), igual que fuente/MV.

## 8. Ciclo de vida y límites

Los detalles de cierre, límites de la versión 1 y verificación se conservan en
[ciclo de vida del sink](hotlap-sink-lifecycle.md).
