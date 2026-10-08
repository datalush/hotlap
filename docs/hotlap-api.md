# Hotlap — API embebida y métricas in-process (SP5)

- Fecha: 2026-10-08
- Estado: implementado (SP5), tests verdes
- Alcance: superficie embebida `hotlap-runtime::Session`, `SessionConfig`,
  registro de métricas in-process (`hotlap-core::MetricsRegistry`).
- Diseño: `2026-10-08-hotlap-api-cli-observability-design.md` (local, fuera del repo).
- CLI: `docs/hotlap-cli.md`. Kata: SP5.

> Código y comentarios en **inglés**; este documento en español.

## 1. Propósito

SP5 cierra la **API embebida**: un único handle síncrono,
`hotlap-runtime::Session`, que envuelve la capa SQL (`SqlSession`) y el motor
(`EngineHandle`), y expone además las **métricas in-process**. El objetivo es
que un integrador use Hotlap desde Rust sin conocer el hilo del motor, los
canales de comandos ni la `SessionContext` de DataFusion.

## 2. Dónde vive cada pieza

| Pieza | Ubicación | Rol |
| --- | --- | --- |
| `Session` | `crates/hotlap-runtime/src/session/api.rs` | Superficie final, síncrona. |
| `SessionConfig` | `crates/hotlap-runtime/src/session_config.rs` | Configuración de `open`. |
| `SqlSession` | `crates/hotlap-runtime/src/session.rs` (+ `session/*`) | Parseo/DDL, catálogo, ciclo de vida del motor, consultas. |
| `MetricsRegistry` | `crates/hotlap-core/src/metrics.rs` | Contadores/gauges atómicos compartibles. |

> **Nota:** `SqlSession` **vive en `hotlap-runtime`**, no en `hotlap-sql`.
> `hotlap-sql` conserva solo el parser DDL, el catálogo y la traducción
> `LogicalPlan → Plan`; la sesión que compone todo (motor + registros) se movió
> a `hotlap-runtime`. `Hotlap` (la fachada sobre el core) queda como pieza
> interna de composición.

`hotlap-runtime` re-exporta en su raíz `Session`, `SessionConfig`,
`SessionError`, `QueryResult`, `SqlSession`, `SourceFactory`, `SinkFactory`,
`FlussSourceFactory` y `FlussSinkFactory`. `MetricsSnapshot` se expone en
`hotlap_runtime::session::MetricsSnapshot` (no en la raíz).

## 3. `SessionConfig`

`SessionConfig::new()` (también `Default`) usa las **factorías Fluss por
defecto** y **sin checkpointing periódico**. Configuración encadenable:

```rust
use hotlap_runtime::SessionConfig;

let config = SessionConfig::new()
    // Fuentes/sinks inyectables (los tests usan fakes aquí).
    // .with_source_factory(...)
    // .with_sink_factory(...)
    // Checkpoint periódico: cada `interval`, conservando `retain` snapshots.
    // .with_checkpoint(interval, retain, backend)
    ;
```

| Método | Efecto |
| --- | --- |
| `new()` / `default()` | Factorías Fluss por defecto, sin checkpointing. |
| `with_source_factory(Arc<dyn SourceFactory>)` | Construye `CREATE SOURCE` con esa factoría. |
| `with_sink_factory(Arc<dyn SinkFactory>)` | Construye sinks con esa factoría. |
| `with_checkpoint(interval, retain, backend)` | Activa checkpointing periódico sobre `backend` (`Box<dyn StateBackend + Send>`), conservando los `retain` más nuevos. |

## 4. `Session`

Un `Session` **posee un runtime Tokio multi-hilo de un worker**
(`new_multi_thread().worker_threads(1)`) con **drivers de IO y tiempo**
(`enable_all`, necesarios para los conectores), de modo que el llamante no
necesita estar dentro de un runtime de Tokio: los métodos **bloquean**
internamente y nunca se invoca el handle del motor desde un executor ajeno.

| Método | Firma | Descripción |
| --- | --- | --- |
| `open` | `fn open(config: SessionConfig) -> Result<Session, SessionError>` | Abre la sesión; el motor no arranca hasta `START`. |
| `sql` | `fn sql(&mut self, sql: &str) -> Result<QueryResult, SessionError>` | Ejecuta una sentencia: DDL, `START` o consulta. |
| `start` | `fn start(&mut self) -> Result<(), SessionError>` | Arranca el motor con el source, las vistas y los sinks declarados. |
| `snapshot` | `fn snapshot(&self, view: &str) -> Result<ZSetBatch, SessionError>` | Lee la salida consolidada de una vista materializada. |
| `metrics` | `fn metrics(&self) -> MetricsSnapshot` | Copia puntual de las métricas (vacía antes de `START`). |
| `checkpoint` | `fn checkpoint(&self) -> Result<u64, SessionError>` | Toma un checkpoint **ahora** y devuelve su id. |
| `shutdown` | `fn shutdown(self) -> Result<(), SessionError>` | Detiene el motor y une su hilo trabajador. |

`QueryResult` distingue:

```rust
pub enum QueryResult {
    Ack(String),                 // DDL/START aceptado
    Rows(Vec<RecordBatch>),      // consulta con resultados Arrow
}
```

### Ciclo de vida típico

```rust
use hotlap_runtime::{Session, SessionConfig};

let mut session = Session::open(SessionConfig::new())?;
session.sql("CREATE SOURCE src WITH (connector='fluss', ...) \
             WATERMARK FOR _event_time AS _event_time - INTERVAL '1 s'")?;
session.sql("CREATE MATERIALIZED VIEW mv AS SELECT k, count(*) FROM src \
             GROUP BY k, tumble(_event_time, INTERVAL '10 s')")?;
session.start()?;

let rows = session.sql("SELECT * FROM mv")?;
let snapshot = session.snapshot("mv")?;
let metrics = session.metrics();
let id = session.checkpoint()?;     // falla si no se configuró checkpointing
session.shutdown()?;
```

## 5. Errores

`SessionError` unifica las fuentes de fallo:

```rust
pub enum SessionError {
    Sql(hotlap_sql::SqlError),   // parseo/planificación/catálogo
    Engine(String),              // motor/runtime/conectores
}
```

`Display` antepone `sql:` / `engine:` respectivamente. Los usos en estado
inválido (p. ej. `DDL after START`) ya se reportan como `SqlError::Unsupported`
desde `hotlap-sql`, por lo que no hay una variante `State` propia. Las consultas
(`Rows`) **no** abortan la sesión; el consumidor decide qué hacer con el error.

## 6. Métricas in-process

`hotlap-core::MetricsRegistry` guarda contadores y gauges con nombre en celdas
`AtomicU64`; se comparte con `Arc` y se actualiza con `&self`:

```rust
use hotlap_core::MetricsRegistry;

let metrics = MetricsRegistry::new();
metrics.inc("rows_ingested");        // +1
metrics.add("rows_ingested", 4);     // +n (contador)
metrics.set("windows_open", 3);      // =n (gauge)
let snap = metrics.snapshot();       // BTreeMap<String, u64> ordenado por nombre
```

`Session::metrics()` devuelve `MetricsSnapshot { entries: BTreeMap<String, u64> }`,
una copia puntual ordenada por nombre; **antes de `START` no hay motor y el mapa
está vacío**.

Métricas que engine/runtime **emiten** en v1 (nombres estables):

| Nombre | Tipo | Significado |
| --- | --- | --- |
| `rows_ingested` | contador | Filas recibidas por el motor (suma por push). |
| `rows_emitted` | contador | Filas de los deltas propagados por los grafos de las vistas, **incluidas las retracciones** (suma por push, sin consolidar). |
| `late_dropped` | contador | Filas descartadas por llegar por debajo del watermark. |
| `late_closed_dropped` | contador | Filas descartadas por caer en ventanas tumbling ya cerradas. |
| `checkpoints_taken` | contador | Checkpoints tomados (periódicos o a demanda). |
| `checkpoints_restored` | contador | Checkpoints restaurados en el arranque/recuperación. |
| `sinks_committed` | contador | Commits de sink coordinados (2PC) confirmados. |

El registro también expone `set` para **gauges**, pero en v1 **ningún** gauge se
emite en producción: las métricas reales son los contadores anteriores (el
ejemplo `windows_open` del diseño quedó fuera del alcance).

## 7. Límites (no-goals v1)

- **Mono-proceso**: una sesión, un motor, un backend de estado local.
- **Sin export externo**: nada de Prometheus/OpenTelemetry; `metrics()` es un
  snapshot in-process.
- **Sin logging/tracing estructurado**.
- La API es **síncrona**; no se ofrece una variante async en v1.
