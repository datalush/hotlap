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
- El kernel `crates/hotlap` no gana dependencias de Arrow ni DataFusion.

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

Residuos de la implementación de SP3, no bloqueantes (parkeados con ruling). Los
más sustantivos tienen issue de kata para repararse; el resto queda aquí:

- **Proyección identidad laxa:** la proyección que DataFusion coloca sobre un
  `Aggregate` se desenvuelve comprobando solo que cada expresión sea una columna
  resoluble; no se valida que sea una identidad exacta (orden/subconjunto de
  columnas). `SELECT count(*), k ...` o `SELECT count(*) ...` se aceptan y su
  orden/subconjunto se descarta en favor del orden normalizado del kernel.
  (Issue kata pendiente.)
- **`built==false` enmascara errores:** el snapshot de una MV usa
  `SnapshotHandle::is_built` (flag de motor, global) y, si el dataflow aún no se
  ha construido, sirve **vacío** en vez de propagar un error del engine anterior
  al primer push. Tolerable para el motor de v1; un motor con estados por input
  necesitaría una señal por input. (Issue kata pendiente.)
- **Escáner DDL minimalista** (`ddl_scan.rs`): reconoce `k='v'` separados por
  comas; no cubre comillas escapadas ni comas dentro de literales. El anclaje de
  `WITH` no es estricto (busca la primera aparición) y la normalización de
  `tumble(...)` es textual (podría casar dentro de un literal).
- **Opción `table` engañosa:** en `CREATE SOURCE`, `table` es una ruta
  `<db>/<table>` (la esperada por `FlussSource::open_from_bootstrap`); el nombre
  del source no se usa. El nombre de la opción sugiere lo contrario.
