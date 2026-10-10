# Ciclo de vida de la sesión SQL Hotlap

Detalle de declaraciones DDL, arranque y retención dinámica. Ver
[superficie SQL Hotlap](hotlap-sql.md).

## Ciclo de vida

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
   snapshot consolidado y lo expanden a un **multiconjunto** (cada peso positivo
   se convierte en tantas filas como indique), sin `DISTINCT` implícito
   (`docs/hotlap-sql-limits.md` §4).

El **DDL se declara antes de `START`** (ventana DDL). Crear un **source** o un
**sink** después de `START` se **rechaza** con `SqlError::Unsupported`. Una
**MV** después de `START` (dynamic view) se admite **solo** con retención de
inputs: usa el mapa de bindings congelado y la retención existente, y **sin
retención se rechaza** (§8, `hotlap-recovery.md`). Se pueden declarar **varias**
fuentes antes de `START`: `START` fija los `InputId` en orden canónico, recompila
cada MV contra esa asignación e ingiere cada fuente con identidad propia. Un
`CREATE SOURCE` duplicado o con nombre ya usado se rechaza sin sustituir la
fuente/watermark vivos (ver `hotlap-cross-source-joins.md`).

Un `START` con **checkpoint durable** consume su configuración (el backend no es
clonable) al arrancar el engine. Si ese arranque falla —recovery rechaza un
checkpoint incompatible, o un source no abre— la sesión queda en estado
**fallido-durable**: un `START` de reintento se rechaza con `SqlError::Unsupported`
en lugar de arrancar sin recovery. Para reintentar hay que **reabrir una sesión
nueva** y reconfigurar el checkpoint explícitamente; un `START` **sin**
checkpoint conserva el reintento normal (recompila contra el registro vigente).
