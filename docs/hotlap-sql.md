# Hotlap — capa SQL/DDL y catálogo mínimo

- Fecha: 2026-10-08
- Estado histórico: implementación documentada al 2026-10-08.
- Alcance: superficie SQL embebida sobre el kernel y los conectores
- Crate: `hotlap-sql` (el kernel `crates/hotlap-core` usa lotes Arrow como frontera)

## 1. Propósito

`hotlap-sql` es una **superficie SQL embebida** mínima sobre el kernel y los
conectores: declara una fuente (`CREATE SOURCE`, con watermark), define vistas
materializadas (`CREATE MATERIALIZED VIEW ... AS SELECT`), arranca el motor con
`START` y consulta las MVs con `SELECT`. El objetivo es el camino vertical
completo `DDL → START → SELECT` sin reimplementar un motor de consultas: se usa
**DataFusion** como parser/planificador y traduce su `LogicalPlan` al `Plan` del
kernel.

## 2. Crate y módulos

`crates/hotlap-sql` es la capa SQL pura (parseo DDL, catálogo y traducción del
plan). La **sesión embebida** (`SqlSession`, `FlussSourceFactory`, MV como
`TableProvider`) vive ahora en `hotlap-runtime` (`session.rs` + `session/`), que
compone esta crate con el runtime y las métricas.

| Módulo | Rol |
| --- | --- |
| `ddl/mod.rs` (+ `ddl/sink.rs`, `ddl_scan.rs`) | parser mínimo de `CREATE SOURCE` / `CREATE SINK` / `CREATE MATERIALIZED VIEW` / `START` |
| `translate.rs` (+ `translate_expr.rs`, `tumble.rs`) | `LogicalPlan` de DataFusion → `Plan` del kernel |
| `convert.rs` | filas del kernel (`Row`/`Scalar`) → `RecordBatch` Arrow |
| `mv_schema.rs` | esquema de salida de una MV según el contrato del kernel |
| `watermark.rs` | parseo de `INTERVAL` y resolución de columna de tiempo de evento |
| `catalog.rs` | registro de fuentes y MVs declaradas |
| `error.rs` | `SqlError` (`Parse` / `Unsupported` / `Catalog` / `Engine`) |

Tipos re-exportados en `lib.rs`: `Catalog`, `MvDef`, `SourceDef`, `CreateSink`,
`SqlError`. La sesión y sus tipos (`SqlSession`, `QueryResult`, `Snapshotter`,
`SourceFactory`, `FlussSourceFactory`) se re-exportan desde `hotlap-runtime`.

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

## 4. `LogicalPlan` de DataFusion como IR hacia `Plan` del kernel

DataFusion planifica cada `SELECT` y `translate::to_kernel_plan` mapea su
`LogicalPlan` al IR del kernel. **Subconjunto soportado**:

| `LogicalPlan` | `Plan` del kernel | Condición |
| --- | --- | --- |
| `TableScan` | `Source` | — |
| `Filter` | `Filter` | predicado `col <op> literal` (`=,<>,<,<=,>,>=`), `AND`/`OR`/`NOT`, `IS [NOT] NULL`; el literal puede ir a cualquiera de los dos lados |
| `Projection` | `Project` | proyección de columnas; la proyección identidad sobre un `Aggregate` se desenvuelve |
| `Aggregate` | `GroupAggregate` / `TumbleCount` | `count`/`sum`/`min`/`max`/`avg` (con `GROUP BY` de columnas); `count(*)` por ventana con, como mucho, un `tumble(col, size)` |
| `Join` | `Join` | inner equi-join; hasta **dos fuentes distintas** (ver `hotlap-cross-source-joins.md`) |

Tipos de columna admitidos: `Int32`, `Int64`, `Float64`, `Utf8` y `Boolean`.
`min`/`max` son **solo numéricos** (`Int32`/`Int64`/`Float64`): sobre `Utf8`
se rechazan; `sum` es `Int32`/`Int64`/`Float64` y `avg` es numérico. Para
`WHERE`, la **semántica NULL** es de **tres valores** (Kleene): una comparación con un
operando nulo da «desconocido» y la fila se excluye del `WHERE` (`NULL AND TRUE`
es nulo, `NULL OR TRUE` es verdadero, `NOT NULL` es nulo).

La **lectura** (`SELECT ... FROM <mv>`) sigue delegando en **DataFusion**: el
kernel solo mantiene el estado incremental de las vistas; el subconjunto de
expresiones de arriba aplica al `WHERE`/agregados de la **view** que se
mantiene, no al `SELECT` de consulta.

Todo lo demás se **rechaza explícitamente** con `SqlError::Unsupported`, nunca
con una traducción parcial o silenciosa:

- **Expresiones calculadas** (`a + 1`) y **funciones/casts** explícitos en la
  view; tampoco se admiten predicados entre dos columnas.
- `HAVING`, `DISTINCT` (`count(DISTINCT ...)`), `FILTER`, `ORDER BY`/`LIMIT`
  dentro de la view, y agregados con modificadores (`DISTINCT`/`FILTER`/
  `ORDER BY`/`NULL TREATMENT`).
- Agregados sin `GROUP BY` (toda operación de grupo del kernel exige clave).
- `min`/`max` sobre `Utf8` (solo numéricos), al igual que `sum`/`avg` sobre
  tipos no numéricos.
- Más de un `tumble`, o agregados de ventana distintos de `count(*)`.
- **Joins no-equi** (o con `filter`), `OUTER`, con más de dos fuentes distintas o
  con condiciones residuales en el `ON`.

Los tipos de columna que el kernel no representa también se rechazan: el
esquema de salida de la MV se valida con `convert::ensure_kernel_types` en el
propio `CREATE MATERIALIZED VIEW`, de modo que un tipo no representable falla en
DDL y no más tarde en el `SELECT`.

## 5. MV como `TableProvider`

Tras `START`, cada MV declarada se registra en el `SessionContext` de
DataFusion como `MvTableProvider`, de modo que un `SELECT ... FROM <mv>` se
planifica como una tabla normal:

- `scan()` lee el snapshot consolidado del motor
  (`Snapshotter::snapshot`) en un hilo bloqueante
  (`tokio::task::spawn_blocking`, porque el handle del motor usa
  `blocking_recv`), lo convierte a **un** `RecordBatch` con
  `convert::zset_to_batch` y lo envuelve en un ejecutor en memoria. La
  conversión **expande** los pesos positivos del snapshot en filas repetidas
  (semántica de multiconjunto, sin `DISTINCT` implícito); un peso negativo falla
  y la expansión está acotada. Detalles y límites en
  `docs/hotlap-sql-limits.md` §4.
- El **esquema** de la MV (`mv_schema.rs`) sigue el contrato de salida del
  kernel: `key ++ [window_start, count]` para `TumbleCount` y
  `key ++ [count, sum, min, max, avg]` para `GroupAggregate`. Los nombres de las
  columnas agregadas son `count`/`sum`/`min`/`max`/`avg` y sus tipos se derivan
  del tipo de la columna de entrada (`count` → `Int64`; `sum` entero → `Int64`;
  `sum` float → `Float64`; `avg` → `Float64`; `min`/`max` conservan el tipo
  numérico de entrada); los nombres de las columnas clave se toman del esquema
  del source en los índices del `key`.
- Una MV sin filas devuelve un snapshot vacío con su esquema; una vista restaurada
  devuelve el estado recuperado.

## 6. Ciclo de vida

El recorrido DDL, arranque, sesiones con checkpoint y vistas dinámicas se detalla
en [ciclo de vida SQL](hotlap-sql-lifecycle.md).

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

## 8. Vistas dinámicas (retención de entradas)

Crear MVs **después** de `START` (*dynamic views*) se admite **solo** con
retención de inputs: el engine reconstruye la vista sobre los deltas retenidos en
orden y la une al flujo vivo. La retención está **apagada por defecto**, así que
sin ella (o si está truncada) la MV tardía se **rechaza** (`Unsupported`) en vez
de devolver un resultado parcial. Detalles en `hotlap-recovery.md`.

## 9. Fuera de alcance, límites y verificación

Los límites conocidos y la cobertura de pruebas se mantienen en
[límites SQL](hotlap-sql-limits.md); joins entre dos fuentes y su recorrido hasta
checkpoint se detallan en [joins entre fuentes](hotlap-cross-source-joins.md).
