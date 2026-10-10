# Hotlap — límites, no-goals y verificación de la capa SQL

- Fecha: 2026-10-09
- Estado: implementado; tests verdes
- Complementa `docs/hotlap-sql.md` (gramática y traducción) y
  `docs/hotlap-cross-source-joins.md` (joins entre dos fuentes).

> Código y comentarios en **inglés**; este documento en español.

## 1. No-goals

- Planificador o motor de consultas propio: se delega en DataFusion.
- Superficie SQL completa: solo el subconjunto de `hotlap-sql.md`; el resto se
  rechaza.
- Catálogo persistente o multisesión: el `Catalog` es en memoria y por sesión.
- Escritura (`Sink`), 2PC o persistencia de estado: fuera de esta capa.
- Dynamic views / `CREATE MV` tras `START`: cubierto por la durabilidad.
- El kernel `crates/hotlap` conserva su propia representación columnar; Arrow y DataFusion
  se usan en las fronteras y en la capa SQL.

## 2. Verificación

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Cobertura en `hotlap-sql`: unit (`ddl`, `watermark`, `convert`, `translate`,
`mv_schema`, `catalog`) e integración (`tests/predicate_translate.rs`,
`tests/predicate_differential.rs`, `tests/cross_source_schema.rs`). La sesión
embebida y el ciclo de vida se verifican en `hotlap-runtime`
(`tests/sql_e2e.rs`, `tests/sql_session_guards.rs`, `tests/sql_cross_source*.rs`).

La ruta Fluss por defecto (`FlussSourceFactory`) requiere un clúster vivo; en
este entorno **no** hay uno, así que se verifica en compilación y los tests
ejercitan el factory inyectado. La integración contra un clúster real queda
pendiente.

## 3. Límites conocidos (v1, low priority)

Residuos de la implementación, no bloqueantes (parkeados con ruling). El resto
queda aquí:

- **Proyección identidad laxa:** la proyección que DataFusion coloca sobre un
  `Aggregate` se desenvuelve comprobando solo que cada expresión sea una columna
  resoluble; no se valida que sea una identidad exacta (orden/subconjunto de
  columnas). `SELECT count(*), k ...` o `SELECT count(*) ...` se aceptan y su
  orden/subconjunto se descarta en favor del orden normalizado del kernel.
- **Snapshot de vista vacía:** una vista sin filas produce un snapshot vacío con
  su esquema; las vistas restauradas también exponen el estado recuperado.
- **Escáner DDL minimalista** (`ddl_scan.rs`): reconoce `k='v'` separados por
  comas; no cubre comillas escapadas ni comas dentro de literales. El anclaje de
  `WITH` no es estricto (busca la primera aparición) y la normalización de
  `tumble(...)` es textual (podría casar dentro de un literal).
- **Opción `table` engañosa:** en `CREATE SOURCE`, `table` es una ruta
  `<db>/<table>` (la esperada por `FlussSource::open_from_bootstrap`); el nombre
  del source no se usa. El nombre de la opción sugiere lo contrario.

## 4. Multiconjuntos SQL (bag) y cota de expansión

El `SELECT ... FROM <mv>` de la superficie SQL usa **semántica de multiconjunto
estándar** (bag), **sin `DISTINCT` implícito**. El motor mantiene el snapshot
**consolidado** de una vista como un Z-set: como mucho una fila por valor
distinto, ordenada, con los pesos cero eliminados. El conversor
(`hotlap-sql/src/convert.rs`, `zset_to_batch`) **expande** cada peso positivo `w`
en `w` filas idénticas antes de entregarlas a DataFusion, de modo que
`SELECT`, `COUNT(*)`, `SUM`/`AVG` y demás agregados ven **todas** las
repeticiones. Ejemplo: un join `2 × 3` produce un snapshot con una fila de peso
6 y la consulta devuelve **seis** filas (y `COUNT(*) = 6`); no una fila distinta.

Contrato y rechazos (errores tipados `SqlError::Unsupported`, nunca `abs`,
ignorados ni truncados):

- **Peso negativo inválido:** un peso `< 0` no describe un multiconjunto y la
  conversión **falla**. Un *changelog* incremental sí admite deltas negativos
  (retracciones); un *snapshot* válido para SQL, no. Son conceptos distintos y
  el rechazo es explícito.
- **Peso cero:** se elimina de la salida (ya lo elimina la consolidación del
  motor).
- **Cota de expansión:** la suma de pesos se calcula con **aritmética
  checked** (el desbordamiento de `i64` falla) y se compara con
  `MAX_SNAPSHOT_ROWS = 1_048_576` **antes** de reservar ningún índice. Un
  snapshot que expanda a más filas falla con error tipado. La cota acota el
  **número de filas**, no los bytes: un esquema ancho o de ancho variable puede
  consumir bastante memoria aunque respete la cota, así que **no** es una
  garantía de memoria. La cota es fija; no se expone configuración porque no se
  necesitó.
- **La cota se aplica antes de cualquier `LIMIT` de consulta:** el proveedor
  materializa el snapshot completo en un `RecordBatch`, así que un
  `SELECT ... LIMIT n` no evita la expansión; si el snapshot excede la cota, la
  consulta falla aunque el `LIMIT` fuera pequeño.

El snapshot es la salida **consolidada** del motor
(`hotlap-engine/src/core/output.rs`), verificado en
`hotlap-sql/src/convert.rs` (unit) y `hotlap-runtime/tests/sql_cross_source_bag.rs`
(paridad de `SELECT`/agregados contra un `VALUES` de DataFusion independiente).
