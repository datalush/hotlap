# Hotlap — capa SQL/DDL y catálogo mínimo (SP3)

- Fecha: 2026-10-07
- Estado: implementado (SP3), tests verdes
- Alcance: superficie SQL embebida sobre el kernel + connectors
- Crate: `hotlap-sql` (el kernel `crates/hotlap` sigue sin Arrow)
- Plan: SP3 (`2026-10-07-hotlap-sql-ddl-catalog.md`)

## 1. Propósito

`hotlap-sql` es una **superficie SQL embebida** mínima sobre el kernel y los
conectores: declara un source (`CREATE SOURCE`, con watermark), define vistas
materializadas (`CREATE MATERIALIZED VIEW ... AS SELECT`), arranca el motor con
`START` y consulta las MVs con `SELECT`. El objetivo es el camino vertical
completo `DDL → START → SELECT` sin reimplementar un motor de consultas: se usa
**DataFusion** como parser/planner y se traduce su `LogicalPlan` al `Plan` del
kernel.

## 2. Crate y módulos

`crates/hotlap-sql`, biblioteca pública mínima:

| Módulo | Rol |
| --- | --- |
| `session/` (`mod.rs`, `runtime.rs`, `fluss_factory.rs`) | `SqlSession`: dispatch de sentencias, ciclo de vida del motor, registro de MVs, y `FlussSourceFactory` (fuente por defecto) |
| `ddl.rs` (+ `ddl_scan.rs`) | parser mínimo de `CREATE SOURCE` / `CREATE MATERIALIZED VIEW` / `START` |
| `translate.rs` (+ `translate_expr.rs`, `tumble.rs`) | `LogicalPlan` de DataFusion → `Plan` del kernel |
| `convert.rs` | filas del kernel (`Row`/`Scalar`) → `RecordBatch` Arrow |
| `mv_provider.rs` (+ `mv_schema.rs`, `session_source.rs`) | MV como `TableProvider` DataFusion |
| `watermark.rs` | parseo de `INTERVAL` y resolución de la columna event-time |
| `catalog.rs` | registro de sources y MVs declarados |
| `error.rs` | `SqlError` (`Parse` / `Unsupported` / `Catalog` / `Engine`) |

Tipos re-exportados en `lib.rs`: `Catalog`, `MvDef`, `SourceDef`, `SqlError`,
`MvTableProvider`, `SqlSession`, `QueryResult`, `Snapshotter`, `SourceFactory`,
`FlussSourceFactory`.

## 3. Gramática DDL mínima

Un parser de strings (no un parser SQL completo) reconoce exactamente estas
formas; cualquier otra cosa produce `SqlError::Parse`. El punto y coma final es
opcional.

```sql
-- 1. Source con watermark
CREATE SOURCE <nombre>
  WITH (k='v', ...)
  WATERMARK FOR <col> AS <col> - INTERVAL '<n> <unidad>';

-- 2. Vista materializada
CREATE MATERIALIZED VIEW <nombre> AS
  SELECT <claves>, count(*)
  FROM <source>
  GROUP BY <claves>, tumble(<col-tiempo>, INTERVAL '<n> <unidad>');

-- 3. Arranque explícito
START;

-- 4. Consulta (planificada directamente por DataFusion)
SELECT ... FROM <mv>;
```

Unidades de `INTERVAL` aceptadas: `ms`, `s`, `min`/`m`, `h` (`watermark.rs`).
El `size` de `tumble` y el `lag` del watermark se convierten a milisegundos
antes de planificar: `tumble.rs` reescribe textualmente
`tumble(<col>, INTERVAL '<...>')` a `tumble(<col>, <ms>)`, porque DataFusion no
tiene un literal de intervalo para estas formas compactas.

## 4. `LogicalPlan` como IR y traducción al `Plan` del kernel

DataFusion planifica cada `SELECT` y `translate::to_kernel_plan` mapea su
`LogicalPlan` al IR del kernel. **Subconjunto soportado**:

| `LogicalPlan` | `Plan` del kernel | Condición |
| --- | --- | --- |
| `TableScan` | `Source` | — |
| `Filter` | `Filter` | predicado `col = literal` o `col > literal Int64` |
| `Projection` | `Project` | proyección de columnas; la proyección identidad sobre un `Aggregate` se desenvuelve |
| `Aggregate` | `GroupCount` / `TumbleCount` | exactamente un `count(*)`; grupo formado por columnas y, como mucho, un `tumble(col, size)` |
| `Join` | `Join` | inner equi-join; ambos lados leen **el mismo** source |

Todo lo demás se **rechaza explícitamente** con `SqlError::Unsupported`, nunca
con una traducción parcial o silenciosa: agregados distintos de `count(*)`
(`sum`, `avg`, `count(DISTINCT ...)`, `FILTER`, `ORDER BY`), operadores de
predicado fuera de `Eq`/`Gt`, más de un `tumble`, o un join entre sources
distintos. Los tipos de columna que el kernel no representa
(`Int64`/`Utf8`/`Boolean` son los admitidos) también se rechazan: el esquema de
salida de la MV se valida con `convert::ensure_kernel_types` en el propio
`CREATE MATERIALIZED VIEW`, de modo que un tipo no representable falla en DDL y
no más tarde en el `SELECT`.

## 5. MV como `TableProvider`

Tras `START`, cada MV declarada se registra en el `SessionContext` de
DataFusion como `MvTableProvider`, de modo que un `SELECT ... FROM <mv>` se
planifica como una tabla normal:

- `scan()` lee el snapshot consolidado del motor
  (`Snapshotter::snapshot`) en un hilo bloqueante
  (`tokio::task::spawn_blocking`, porque el handle del motor usa
  `blocking_recv`), lo convierte a **un** `RecordBatch` con
  `convert::rows_to_batch` y lo envuelve en un ejecutor en memoria.
- El **esquema** de la MV (`mv_schema.rs`) sigue el contrato de salida del
  kernel: `key ++ [window_start, count]` para `TumbleCount` y
  `key ++ [count]` para `GroupCount`. Los nombres de las columnas clave se
  toman del esquema del source en los índices del `key`.
- Si el dataflow aún no se ha construido (ningún batch ingerido), el snapshot
  se sirve como **vacío** en vez de error (`SnapshotHandle::is_built`), de forma
  que una MV recién arrancada responde 0 filas sin colgarse.

## 6. Ciclo de vida

1. `CREATE SOURCE` construye el `Source` vía el `SourceFactory` inyectado
   (`SqlSession::open()` usa `FlussSourceFactory`, que mapea
   `connector='fluss'` + `bootstrap` + `table` a
   `FlussSource::open_from_bootstrap`, donde `table` es una ruta
   `<db>/<table>` y el nombre del source no se usa; los tests inyectan un
   factory propio con `SqlSession::open_with_factory(...)`), valida la columna
   de watermark y registra una tabla de planificación.
2. `CREATE MATERIALIZED VIEW` planifica el `SELECT`, lo traduce, valida que los
   tipos de salida sean representables y **solo entonces** registra la
   definición, el `Plan` y el esquema; una vista rechazada no deja su nombre en
   el catálogo, así que un reintento con el mismo nombre no falla con un
   `view already exists` engañoso.
3. `START` construye el `Pipeline` (source + watermark + vistas), arranca el
   `EngineHandle` y registra los `MvTableProvider`.
4. Los `SELECT` se ejecutan con DataFusion; las consultas a MVs leen el
   snapshot consolidado.

El **DDL se declara antes de `START`** (ventana DDL). Crear un source o una MV
después de `START` se **rechaza** con `SqlError::Unsupported` (no se ignora ni
se aplica parcialmente). Un **segundo `CREATE SOURCE`** también se rechaza con
`SqlError::Unsupported("only one source is supported in v1")`: v1 admite una
única fuente por sesión, y aceptarla sobrescribiría la fuente/watermark vivos
dejando el primer nombre registrado apuntando a los datos del segundo.

## 7. Nota sobre `_event_time`

La columna event-time la **añade el source** (el connector Fluss la agrega desde
el timestamp del broker; el `Source` la reporta vía
`event_time_column()`). Por eso el DDL la referencia por nombre:

```sql
CREATE SOURCE src WITH (connector='fluss', ...)
  WATERMARK FOR _event_time AS _event_time - INTERVAL '5 s';
```

El `time_col` declarado en `WATERMARK FOR` se resuelve contra el esquema real
del source; si no existe, la creación falla. El kernel recibe el índice y el
`lag` derivados, no el texto SQL.

## 8. Límite v1: dynamic views

Crear MVs **después** de `START` (*dynamic views*) queda **fuera de v1**: el
`EngineHandle` arranca un dataflow diferencial ya construido y no admite
añadirle operadores en caliente sin un motor de estado/replay. Registrar una MV
tardía produciría resultados incorrectos, así que se rechaza explícitamente. Es
un requisito registrado para **SP4**.

## 9. No-goals

- Planificador o motor de consultas propio: se delega en DataFusion.
- Superficie SQL completa: solo el subconjunto descrito; el resto se rechaza.
- Catálogo persistente o multisesión: el `Catalog` es en memoria y por sesión.
- Escritura (`Sink`), 2PC o persistencia de estado: SP4.
- Dynamic views / `CREATE MV` tras `START`: SP4.
- El kernel `crates/hotlap` no gana dependencias de Arrow ni DataFusion.

## 10. Verificación

```bash
cargo fmt --all -- --check
cargo clippy -p hotlap-sql --all-targets -- -D warnings
cargo test -p hotlap-sql
cargo test -p hotlap
cargo test -p hotlap-connectors
```

Cobertura: unit (`ddl`, `watermark`, `convert`, `translate`, `mv_schema`,
`catalog`) e integración (`tests/e2e.rs` — paridad del resultado SQL con una
recomputación completa de las ventanas tumbling, MV vacía → 0 filas, DDL tras
`START` rechazado, y drop de la sesión sin pánico en el executor;
`tests/session_guards.rs` — segundo `CREATE SOURCE` rechazado, tipo de salida
de MV no representable rechazado en DDL, y nombre de vista no envenenado por un
`CREATE MATERIALIZED VIEW` fallido).

La ruta Fluss por defecto (`FlussSourceFactory`) requiere un clúster vivo; en
este entorno **no** hay uno, así que se verifica en compilación y los tests
ejercitan el factory inyectado. La integración contra un clúster real queda
pendiente.

## 11. Límites conocidos (v1, low priority)

- La proyección que DataFusion coloca sobre un `Aggregate` se desenvuelve
  comprobando solo que cada expresión sea una columna resoluble; no se valida
  que sea una identidad exacta (orden/subconjunto de columnas).
- El escáner DDL (`ddl_scan.rs`) es minimalista (reconoce `k='v'` separados por
  comas); no cubre comillas escapadas ni comas dentro de literales.
