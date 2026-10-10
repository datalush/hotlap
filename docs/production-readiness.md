# Estado de preparación para producción

Los providers nativos de DataFusion leen logs Fluss y estado KV vigente, e
implementan SQL `INSERT INTO` mediante los writers Rust existentes de Fluss.
Sus garantías de consistencia se describen en
[semántica de lectura](reading-semantics.md). Un scan KV tiene un snapshot
**por bucket**, no un snapshot atómico entre buckets.

El alcance activo es Rust nativo y validación de providers DataFusion. Su
aceptación no certifica el runtime de Hotlap. Se retiraron paquetes FFI/Python e
integración experimental con host; sus verificaciones históricas no constituyen
aceptación actual. La evidencia nativa de permisos/fallos, ciclo de vida del motor
y perfiles sostenidos se registra en
`native-failure-verification.md`, `native-engine-acceptance.md` y
`native-profile-plan.md`. La reproducción Git limpia está registrada aparte; las
garantías de despliegue se limitan al perfil de servidor documentado.

## Evidencia de integración nativa

Los casos de providers, consultas SQL, escrituras y runtime del laboratorio se
conservan en [evidencia nativa](production-native-evidence.md).

## Evidencia de almacenamiento remoto

Los ensayos Docker de retención, S3/STS y fallos HTTP se conservan en
[evidencia remota](production-remote-evidence.md).

## Límites de runtime que configurar

La [matriz nativa de permisos/fallos](native-failure-verification.md) verifica
SQL real SASL/ACL de solo lectura, causas tipadas de autorización, escrituras
parciales, pérdida de socket y recuperación explícita tras reiniciar el mismo
tablet. Su prueba de crash `.6` con una réplica requiere una ventana de checkpoint
del servidor de seis segundos para el prefijo ACKed: un crash inmediato recuperó
antes offset cero. ACK no es checkpoint del motor ni promesa de durabilidad
inmediata ante crash. Promoción de líder replicado y durabilidad en disco requieren
aceptación del perfil de servidor elegido.

El pool de memoria DataFusion predeterminado no tiene límite. Configure un pool
acotado en `RuntimeEnv`, ajuste `target_partitions` (y opcionalmente
`with_max_partitions`) y use timeout positivo de scan. Una reserva cubre **un
lote fuente decodificado por stream activo**; no incluye buffer fetch comprimido
de Fluss, prefetch remoto ni memoria de operadores downstream. Ajuste
`scanner_log_fetch_max_bytes`,
`scanner_log_fetch_max_bytes_for_bucket`,
`scanner_remote_log_prefetch_num` (slots de archivos descargados) y
`scanner_remote_log_max_pending_segments` (límite de solicitudes pendientes,
8192 por defecto) y `scanner_remote_log_max_prefetch_bytes` (bytes remotos por
scanner, 64 MiB por defecto). Segmento remoto sobredimensionado falla el scan:
scanner reserva tamaño anunciado y reserva bytes extra **antes de escribir cada
  chunk descargado**. Fallo/cancelación elimina archivos parciales y libera reserva.
Esto limita temporales remotos del scanner, no a otros usuarios del disco ni
memoria temporal de chunks OpenDAL. Un registro servidor sobredimensionado puede
superar hint de fetch antes de que pool rechace batch Arrow decodificado.
`scanner_remote_log_read_chunk_bytes` (8 MiB por defecto, máximo 64 MiB) define
tamaño de chunk por reader; combínelo con `scanner_remote_log_read_concurrency`
y `remote_file_download_thread_num` (4 y 3 por defecto) al estimar memoria fuera
del pool DataFusion. Por defecto, su producto es 96 MiB de datos potencialmente en
vuelo **por scanner**. Es referencia de dimensionamiento, no
límite estricto de memoria del proceso: descompresión, respuestas fetch y
asignaciones internas OpenDAL son adicionales. Configure
`scanner_remote_log_operation_timeout_ms` (30.000 ms por defecto) para cada
operación remota reader/open/read y fetch/espera de credenciales; no es un
timeout global de consulta.
`scanner_remote_log_max_retries` controla reintentos *tras* el primer intento
(10 por defecto; `0` significa un intento). `scanner_remote_log_retry_backoff_base_ms`
y `scanner_remote_log_retry_backoff_max_ms` configuran backoff exponencial con
jitter (100/5000 ms por defecto); base positiva
y máximo no menor que base, hasta 3.600.000 ms. Solo aplican a fallos reintentables
de descarga remota. Cancelar scan interrumpe reintento en cola, sin esperar backoff.
Timeout DataFusion sigue limitando lectura de fuente completa.

La fuente usa cliente Rust integrado en el repo. La integración fija DataFusion
55.1 y Arrow 59 en `Cargo.lock`, con backport nativo documentado. No sustituya
scan completo de tabla SQL por preview acotado.

## Comandos de verificación

Compilación funcional usa `CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0`.
Cobertura del motor nativo y verificaciones de runtime/planner/UDF del llamador
están en [aceptación del motor nativo](native-engine-acceptance.md).

```bash
cargo fmt --all --check
cargo fmt --manifest-path clients/rust/Cargo.toml --all --check
cargo clippy -p fluss-datafusion --all-targets -- -D warnings
cargo clippy --manifest-path clients/rust/Cargo.toml -p fluss-rs --all-targets -- -D warnings
cargo test -p fluss-datafusion
cargo test --manifest-path clients/rust/Cargo.toml -p fluss-rs --lib client::table::
cargo test --manifest-path clients/rust/Cargo.toml -p fluss-rs --lib client::write::
```

Con laboratorio native-sni aislado activo y credenciales ignoradas en `../lab/.env`:

```bash
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 KUBECONFIG=/tmp/opencode/native-sni.kubeconfig uv run --no-project --env-file ../lab/.env cargo test -p fluss-datafusion --test live_log_sql -- --ignored --test-threads=1
KUBECONFIG=/tmp/opencode/native-sni.kubeconfig CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 uv run --no-project --env-file ../lab/.env cargo test --manifest-path clients/rust/Cargo.toml -p fluss-rs --lib client::table::kv_scanner::tests::leader_restart_invalidates_snapshot_without_restarting_reader -- --ignored
```

El primer comando reinicia **solo** el pod coordinador activo para failover; el
segundo reinicia **solo** un pod tabletserver. Ambos requieren contexto aislado
`k3d-native-sni` y esperan recuperación.

Ejecute perfil Docker de almacenamiento aparte de pruebas native-sni (ambos pueden
usar puerto 9123):

```bash
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.6 CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p fluss-datafusion --locked --test remote_retention datafusion_reads_remote_and_rejects_lost_retention -- --ignored
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.6 CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 uv run --no-project --env-file ../lab/.env cargo test -p fluss-datafusion --locked --test remote_retention datafusion_reads_and_expires_rustfs_s3 -- --ignored
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.6 FLUSS_STS_READONLY_POLICY=1 CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 uv run --no-project --env-file ../lab/.env cargo test -p fluss-datafusion --test remote_retention datafusion_reads_and_expires_rustfs_s3 -- --ignored
```

El tercer comando habilita política limitada al prefijo único de prueba y verifica
**el token devuelto por Fluss**, con GetObject exitoso y PutObject/DeleteObject
denegados. Imagen `.6` publicada también pasó sin `FLUSS_STS_READONLY_POLICY`,
verificando default retrocompatible.

Ejecute aparte el perfil corto de fallos HTTP reales, usando IP host accesible
desde Docker para `FLUSS_FAULT_PROXY_HOST` (host de referencia:
`192.168.68.55`):

```bash
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.6 FLUSS_FAULT_PROXY_HOST=<host-IP> CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 uv run --no-project --env-file ../lab/.env cargo test -p fluss-datafusion --test remote_retention datafusion_handles_real_rustfs_http_failures -- --ignored
```

Ejecute el perfil separado de expiración STS real (~16 min) solo si hace falta
esa verificación larga. Añada `FLUSS_STS_PREFLIGHT=1` para validar setup y parar
antes de expirar:

```bash
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.6 FLUSS_FAULT_PROXY_HOST=<host-IP> CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 uv run --no-project --env-file ../lab/.env cargo test -p fluss-datafusion --test remote_retention datafusion_renews_real_rustfs_sts_after_expiry -- --ignored --nocapture
```

## Perfil de presión de recursos

Resultados y valores medidos del perfil de referencia: [evidencia de presión de
recursos](production-profile-evidence.md).
