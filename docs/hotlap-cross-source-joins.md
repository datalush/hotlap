# Hotlap — joins cross-source en SQL (resolución de relaciones)

- Fecha: 2026-10-09
- Estado: implementado; tests verdes
- Alcance: `INNER JOIN` por igualdad entre **dos fuentes distintas**, de SQL al
  engine y a la durabilidad.
- Código: `hotlap-sql` (`bindings`, `translate`, `mv_schema`), `hotlap-runtime`
  (`sources`, `source_feed`, `source_checkpoint`, `recovery`).
- Complementa `docs/hotlap-sql.md` y `docs/hotlap-recovery.md`.

> Código y comentarios en **inglés**; este documento en español.

## 1. Qué se habilita

Una **materialized view** SQL puede unir dos relaciones declaradas con
`CREATE SOURCE` distintas, siempre que sea un `INNER JOIN` por igualdad (clave
simple o compuesta) sin condiciones residuales en el `ON`. Cada relación
conserva su propia identidad (`InputId`), schema, offsets y watermark; ninguna
tabla colapsa en el input de la otra.

El kernel no cambia: DataFusion sigue siendo el front-end y su `LogicalPlan` se
traduce al `Plan::Join` del IR propio. El join incremental existente produce los
resultados; este slice solo conecta identidades y durabilidad.

## 2. Ejemplo completo

```sql
CREATE SOURCE a WITH (connector='inmem')
  WATERMARK FOR _event_time AS _event_time - INTERVAL '1 s';
CREATE SOURCE b WITH (connector='inmem')
  WATERMARK FOR _event_time AS _event_time - INTERVAL '1 s';

CREATE MATERIALIZED VIEW j AS
  SELECT a.k, b.value FROM a JOIN b ON a.k = b.k;

START;

SELECT k, value FROM j;
```

Secuencia: se declaran las dos fuentes y la MV **antes** de `START`; `START`
congela las asignaciones, registra ambos inputs y arranca el engine; tras
`START`, cada lote de A o B se ingiere en su input y solo entonces se confirma
su offset. `SELECT` lee el snapshot consolidado de la MV (DataFusion).

## 3. Nombres de salida y aliases

- Las relaciones del `FROM` admiten **alias de tabla** (`FROM a AS x`): el alias
  solo afecta la calificación de columnas y resuelve al **mismo** `InputId`.
- Los nombres de las columnas de salida siguen el **schema de las fuentes**: el
  join concatena los campos de cada rama en orden y la proyección conserva los
  campos seleccionados. No se renombran columnas.
- Un **alias de columna** en la proyección (`SELECT a.k AS renamed`) queda
  **fuera del subconjunto soportado** y se rechaza en `CREATE MATERIALIZED VIEW`
  (`SqlError::Unsupported`), porque no es una referencia de columna. La política
  de nombres de salida es la existente: no se altera en este slice.

## 4. Restricciones soportadas y rechazadas

Soportado: `INNER JOIN` equi con clave simple o compuesta, hasta **dos** inputs
distintos, con los predicados, proyecciones y agregados ya admitidos.

Se **rechaza** explícitamente antes de publicar la vista (nunca traducción
parcial o silenciosa):

- `OUTER` y joins **no-equi**.
- Condiciones **residuales** en el `ON` (p.ej. `ON a.k=b.k AND a.ts>b.ts`).
- Joins de **más de dos** fuentes distintas.
- Columnas **ambiguas** sin calificar cuando ambas ramas exponen el mismo nombre.
- Una relación **sin binding** en el registro de fuentes (no se usa un input por
  defecto).
- Alias de **columna** en la proyección (ver §3).

## 5. Identidad de fuentes y splits

`START` asigna `InputId` contiguos en **orden canónico de nombre** y recompila
cada MV desde su plan lógico con esa asignación definitiva. En recovery prevalece
el registro persistido: declarar las mismas fuentes en distinto orden restaura
los mismos IDs.

La identidad efectiva de un split es `(InputId, SplitId)`. Dos fuentes con
`SplitId` 0 **no** comparten offsets ni estado de watermark: cada una mantiene su
propio `SourceState` y su frontier. Un lote de A nunca adelanta el watermark de
B ni descarta datos válidos de B.

## 6. Checkpoint conjunto

El checkpoint contiene el snapshot del engine, el registro durable de fuentes y
los offsets aplicados por fuente, en un **único formato multifuente** (contenedor
`HLSR` + payload del engine) que sirve tanto para una como para dos fuentes. Cada
entrada guarda id, nombre canónico, schema Arrow IPC, lag de watermark, columna
event-time y `SourceState`.

La captura es coherente con la ingesta: no mezcla un snapshot anterior a un push
con offsets posteriores. No implica una transacción distribuida ni un corte
simultáneo en los sistemas externos.

No hay lectores de **formatos anteriores**, migraciones ni fallbacks. Un
checkpoint monofuente previo o de versión/magic **incompatible** —incluida una
versión desconocida del frame interno del engine— produce
`ConnectorError::Unsupported` **fatal**: recovery **no** arranca en vacío ni cae
a un checkpoint anterior. Un schema, nombre o identidad que no valida contra las
fuentes declaradas también es fatal. Solo la **corrupción del formato actual**
(truncado o payload ilegible) es tolerada, cayendo al predecesor válido más
nuevo (SP8).

## 7. Recovery

Antes de restaurar o consumir, recovery **valida** el registro, los IDs y los
schemas contra las fuentes declaradas; una fuente ausente, renombrada o con
schema/lag/columna event-time incompatible falla en este punto.

Después, restaura el engine y reanuda **cada** fuente desde su offset aplicado.
Se conserva el protocolo SP8: un commit pendiente se **promueve** (re-conduce el
commit de los sinks si todos son re-conducibles y publica `valid`) o se
**descarta** con señal explícita y replay desde el checkpoint válido anterior.
No se añade 2PC nuevo ni exactly-once: el techo del sink no cambia.

Un error de lectura, ingesta o ack detiene la ingesta de **todo** el runtime: se
registra el error y no se siguen publicando nuevos resultados ni checkpoints
sobre estado incierto (fail-stop). Una fuente agotada normalmente **no**
deshabilita los checkpoints.

## 8. Semántica NULL (límite actual)

El engine codifica las claves de join con `arrow::row`, de modo que una clave
`NULL` **compara igual** a otra `NULL` y no se descarta. Es una propiedad de la
**codificación de claves del join**, **no** la lógica de predicados **Kleene** de
tres valores que sí aplica a `WHERE` y agregados (donde `NULL = NULL` es
«desconocido» y la fila se excluye). Es la semántica existente del engine en este
slice, **sin ampliarla**: no se añaden expresiones SQL ni se cambian las reglas
del kernel. El límite se caracteriza en
`hotlap-engine/tests/cross_source_null_keys.rs`.

## 9. Fuentes antes y después de `START`

- Declarar una fuente **después** de `START` se **rechaza** (`SqlError::Unsupported`).
- Declarar **otra** fuente **antes** de `START` es válido y **recompila** las MVs
  al arrancar contra la asignación definitiva, de modo que una MV temprana no
  acaba leyendo otra relación.
- Una MV nueva **post-`START`** usa el mapa congelado y la retención existente.

## 10. Verificación

- `hotlap-sql` (`translate/tests/joins.rs`, `tests/cross_source_schema.rs`):
  resolución de cada rama a su input, dos aliases de una misma fuente, clave
  compuesta, columna ambigua, relación sin binding, >2 inputs, residual `ON` y
  schema por input.
- `hotlap-runtime/tests/sql_cross_source.rs` y `sql_cross_source_oracle.rs`: flujo
  `CREATE SOURCE → MV → START → SELECT` y paridad con recomputación completa.
- `cross_source_watermarks.rs`: splits con el mismo número y watermarks
  independientes.
- `sql_cross_source_recovery.rs`, `cross_source_recovery.rs`: restart con
  declaraciones reordenadas y resume de offsets independientes.
- `cross_source_pending.rs` y `cross_source_pending_schema.rs`: SP8
  promovible/descartable con dos fuentes, schema cambiado y formato ajeno antes
  de cualquier commit de sink.
- `cross_source_incompatible_codec.rs`: una versión interna de frame o snapshot
  no soportada es **fatal** (sin fallback ni arranque limpio) antes de leer
  fuentes o confirmar sinks.
- `sql_session_durable_start.rs`: un `START` durable fallido no se reintenta sin
  su checkpoint; se exige una sesión nueva.
- `cross_source_failures.rs`: fail-stop de lectura en una sesión SQL de dos
  fuentes, sin publicar el lote no confirmado. Los modos lectura/push/ack a nivel
  de pipeline se cubren en `runtime_fail_stop*.rs`.
- `hotlap-engine/tests/cross_source_oracle.rs` y `cross_source_null_keys.rs`:
  oráculo con multiplicidades y semántica de claves nulas.
