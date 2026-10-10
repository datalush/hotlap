# Hotlap

Hotlap es un motor de streaming nativo en Rust, con núcleo incremental columnar
(`hotlap-engine`) y superficie SQL/DDL embebida (`hotlap-sql`). La integración de
providers Fluss con DataFusion (`fluss-datafusion`) y el cliente Rust de Fluss
(`fluss-rs`) conservan sus nombres y responsabilidades.

```text
crates/hotlap/             Fachada pública del núcleo incremental
crates/hotlap-core/        Contrato del motor (IR Plan, IncrementalCore, ZSetBatch)
crates/hotlap-engine/      Motor incremental columnar
crates/hotlap-connectors/  SPI Source/Sink y fuente/sink Fluss
crates/hotlap-runtime/     Composición, hilo de motor y checkpoints
crates/hotlap-sql/         Superficie SQL/DDL embebida y catálogo
crates/fluss-datafusion/   Integración de providers Fluss con DataFusion
clients/rust/crates/fluss/ Protocolo nativo, metadatos, routing, codecs Arrow y writers
vendor/datafusion-55.1.0/  Core publicado con backport genérico DELETE/UPDATE documentado
docs/                      Contratos, propiedad de recursos y evidencia de verificación
```

El motor incremental posee estado columnar, lotes Arrow Z-set en su frontera y
codificación de claves por filas detrás del contrato `IncrementalCore` de `hotlap-core`, por lo que
se retiraron las dependencias `differential-dataflow`/`timely`. `hotlap-connectors`
contiene SPI Source/Sink y fuente/sink Fluss; `hotlap-runtime` compone el sistema,
selecciona un núcleo y conduce el hilo del motor, sin que connectors dependa de
un motor concreto. `hotlap-sql` traduce SQL/DDL embebido a planes, incluidos
`INNER JOIN` entre dos fuentes independientes (ver
[joins entre fuentes](docs/hotlap-cross-source-joins.md)). La durabilidad reside
en `hotlap-runtime`: checkpoints y recuperación, coordinación 2PC de sinks con
control de capacidad y vistas materializadas creadas después de `START` (ver
[durabilidad](docs/hotlap-durability.md) y [recuperación](docs/hotlap-recovery.md)).

El cliente Rust importado proviene de `dc427e1290847b4a569b6745fcb87b256292bf6a`.
Se conservan sus licencias y avisos Apache. Java/referencias y bindings no Rust se
retiraron del árbol actual; su proveniencia permanece en historial Git. El cliente
incluye esquema de protocolo para regenerar sin Java. Ver
[estructura y migración Hotlap](docs/hotlap-layout.md) y [notas de historial](docs/history-cleanup.md).

## API nativa y semántica

- `FlussLogTable::open` lee un rango batch finito capturado por ejecución.
- `FlussLogTable::open_with_options(..., LogReadOptions::default())` transmite desde
  offsets retenidos más antiguos; admite mapas explícitos latest/completos.
- `FlussKvTable::open` lee snapshots KV finitos del servidor, uno por bucket y
  no una transacción atómica entre buckets.
- Registre providers explícitamente o cargue `FlussCatalog` snapshot opcional.
- SQL `INSERT INTO` agrega a logs o hace upsert KV de fila completa, finito o
  continuo. Writers nativos gestionan particiones/layouts efectivos mixtos.
- DELETE KV SQL selecciona claves snapshot con predicados exactos DataFusion y
  requiere política de tabla que lo permita. MERGE usa operadores nativos
  join/filter/CASE, exige fuente finita y semántica KV ordinaria de reemplazo;
  no cambia PK/claves partición ni admite INSERT con columnas parciales.
- `subscribe_progress()` observa posiciones fuente ofrecidas/excluidas;
  `subscribe_writes()` observa ACK y resultados parciales conservadores. Ninguno
  es checkpoint de motor. Conteos finales SQL aparecen solo tras EOF/éxito.

No hay rollback de sentencia, escritura condicional, snapshot globalmente
consistente, replay automático del trabajo ni commit exactly-once source/sink.
Motor/aplicación poseen política de pool, concurrencia, estado del trabajo,
checkpoints y conciliación. Leases source/sink y guards nativos contabilizan
recursos retenidos en el pool DataFusion real, no en otro asignador ni como límite
RSS del proceso.

La pila sigue en DataFusion 55.1 / Arrow 59. El backport core conserva selección
vacía optimizada DELETE/UPDATE y rechaza restricciones de fila no soportadas; ver
[proveniencia vendor](vendor/README.md).

El ejemplo nativo usa provider explícito, parser/planner/operadores DataFusion
normales y pool host acotado. Imprime resultados lote a lote, no los acumula en
un Vec de salida ilimitado:

```sh
FLUSS_BOOTSTRAP=localhost:9123 DATAFUSION_POOL_MIB=64 DATAFUSION_TARGET_PARTITIONS=2 CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo run --locked -p fluss-datafusion --example native_query -- my_database my_table log 'SELECT COUNT(*) FROM fluss_source'
```

Use `kv` para tablas con clave primaria. Ajustes TLS/SASL Fluss usan `FLUSS_CA_FILE`,
`FLUSS_USER` y `FLUSS_PASSWORD`; memoria/concurrencia del motor usan por separado
ajustes `DATAFUSION_*`. El SQL predeterminado es COUNT; DataFusion planifica el SQL
indicado con nombre registrado `fluss_source`.

## Compilación y verificación

Builds funcionales usan DEBUG y ocho jobs. RELEASE se reserva para perfiles/entrega.

```sh
cargo metadata --no-deps --format-version 1
cargo metadata --no-deps --format-version 1 --manifest-path clients/rust/Cargo.toml
cargo fmt --all -- --check
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --locked
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR="$PWD/target" cargo test --manifest-path clients/rust/Cargo.toml -p fluss-rs --locked --lib
```

Pruebas native-sni usan credenciales exportadas `FLUSS_BOOTSTRAP`, `FLUSS_CA_FILE`,
`FLUSS_USER` y `FLUSS_PASSWORD`. Crean tablas aisladas y las limpian. Un launcher
opcional carga `.env` ignorado del laboratorio sin dependencias Python/virtualenv
en este proyecto:

```sh
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 uv run --no-project --env-file ../lab/.env cargo test -p fluss-datafusion --locked --test write_sql -- --ignored --test-threads=1
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.6 CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p fluss-datafusion --locked --test write_pressure -- --ignored --test-threads=1
```

Perfiles largos de almacenamiento/STS/recursos tienen comandos opt-in y prefijos
aislados; consulte evidencia antes de repetirlos. Credenciales y artefactos locales
de build nunca forman parte del repositorio.

## Alcance de aceptación actual

Arquitectura, lecturas y escrituras finitas/continuas tienen hitos verificados de
alcance acotado. Las comprobaciones funcionales/fallos nativos y perfiles sostenidos
finales cuentan con evidencia delimitada. Verificación Git limpia está
[registrada](docs/native-checkout-verification.md), incluido smoke remoto exitoso
tras recuperar endpoint RustFS existente. El motor bajo aceptación es **DataFusion
nativo en este repo**: SessionState/RuntimeEnv del llamador, planificación/
operadores, concurrencia/contrapresión, cancelación/reejecución y recuperación.
Pruebas de providers por sí solas no establecen esas garantías. FFI/Python no es
fase activa ni dependencia de validación Rust.

- [Contrato Rust canónico](docs/rust-contract.md)
- [Semántica de lectura](docs/reading-semantics.md)
- [Auditoría e historial de implementación](docs/rust-implementation-audit.md)
- [Evidencia de preparación para producción](docs/production-readiness.md)
- [Presión de lectura](docs/read-pressure-verification.md)
- [Presión de escritura](docs/write-pressure-verification.md)
- [Observaciones de escritura](docs/write-observation-contract.md)
- [DELETE](docs/delete-contract.md) y [MERGE](docs/merge-contract.md)
- [Aceptación de INSERT continuo](docs/streaming-write-acceptance.md)
- [Aceptación del motor nativo](docs/native-engine-acceptance.md)
- [Fallos nativos](docs/native-failure-verification.md)
- [Perfiles de la ruta final](docs/native-profile-plan.md)
