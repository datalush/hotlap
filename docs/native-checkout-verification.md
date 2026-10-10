# Verificación desde checkout Git limpio nativo

Los SHA originales de las ejecuciones se conservan como evidencia histórica.
Esto no cambia la fuente nativa probada ni elimina el fallo/recuperación inicial registrado.

## Fuente versionada

Checkout detached verificado **`e12c2f79fd639f9ced67af9e97c5c9ee33ead64a`**, con árbol limpio antes y después. Es un worktree Git real, no uno de los exports de fuentes sin commit anteriores. La serie de fuentes es:

| Commit | Alcance |
| --- | --- |
| `2a85deb` | Retirada integración Python/FFI activa; workspaces y locks nativos |
| `043a346` | Preservación causas autorización; permisos reales y recuperación socket |
| `1a4556b` | Cobertura sesión/runtime/operadores DataFusion llamador y ejemplo nativo |
| `4b78eca` | Métricas nativas y harnesses de perfiles sostenidos lectura/escritura/continuos |
| `e12c2f7` | Evidencia acotada de perfiles, asignación y limpieza |

Ubicación checkout de esta ejecución: `/tmp/opencode/native-commit-checkout`. El directorio target era externo (`/tmp/opencode/native-repro-target-a7739086`) y reutilizó dependencias/artefactos ya descargados. No se copiaron target, virtualenv, wheel, credenciales ni fuentes ignoradas al checkout. Builds usaron `--offline --locked`, DEBUG, ocho jobs y `CARGO_PROFILE_DEV_DEBUG=0`. Evidencia anterior de export registra también rebuild con target vacío; son comprobaciones distintas.

## Comprobaciones desde checkout limpio

| Comprobación | Resultado |
| --- | --- |
| Pruebas predeterminadas workspace raíz | 29 core + 3 planner genérico aprobadas; 26 integraciones opt-in inicialmente ignoradas |
| Pruebas independientes biblioteca cliente | 836 aprobadas, 2 ignoradas; tiempo prueba 0,86s |
| SQL escritura nativa | 8 aprobadas en 42,57s, incluido ejemplo nativo log/KV ejecutado aparte |
| SQL lectura live nativa | 9 aprobadas en 79,63s, incluido failover coordinador aislado |
| Fixture SASL/ACL propio | 1 aprobada en 5,70s |
| Matriz propia de saturación/recuperación socket | 2 aprobadas en 121,78s |
| Smoke funcional de cuatro casos writer/routing/proyección | Aprobado en 19,26s, calentamiento 1s/medición 2s |
| Smoke funcional reader no acotado/INSERT concurrente real | Aprobado en 8,11s, 1s/2s, prefijo completo exacto y cancelación |
| Smoke pressure lectura remota RustFS (tras recuperar endpoint) | Aprobado en 14,66s desde `6f9749f` limpio, 64 filas log, 1s/2s |
| Build/ejecución ejemplo nativo | Build offline; conteos log/KV 6/1 con pool 16 MiB y 2 particiones destino |
| Clippy raíz y cliente/test-cluster | All-targets pasó con `-D warnings` |
| Formato raíz/cliente y diff check | Aprobados |

Solo se eliminaron fixtures propios de tablas/contenedores. Failover native-sni usa kubeconfig/namespace aislados. Credenciales/CA de laboratorio siguen siendo entradas externas.

## Grafo de dependencias y comparación de código medido

Se comprobaron ambos grafos resueltos con lock: raíz 453 paquetes, cliente 431. Sus workspaces son `fluss-datafusion` y las cuatro crates nativas del cliente.
No hay PyO3, arrow-pyarrow, datafusion-ffi/datafusion-ffi-ext/datafusion-python-util, Stabby, Rustler ni bridge Python del proyecto activo. Raíz fija DataFusion 55.1.0/Arrow 59.3.0; lock independiente cliente fija Arrow 59.0.0. Al enlazarse desde raíz, cliente usa lock raíz; fuentes bindings importadas siguen excluidas.

Se compararon 237 archivos fuente/manifests/locks/planner/perfiles nativos con último export verificado `4ac0ada63aefeb5f7e635e62ad4f058772215117ef66a8738b47e50b31e82103`:
**236 idénticos byte a byte; un cambio solo de ajuste de comentario en `metrics.rs` cliente; sin diferencias ejecutables**. Las ejecuciones RELEASE anteriores conservan su alcance medido: cuatro casos writer
60s/300s, reader finito 5+30 min, reader nativo no acotado separado 60s/300s y
controles de asignación. El modo no acotado se añadió tras primera matriz writer y
estuvo desactivado para esa carga; corrección lint divisibilidad es equivalente. Ningún cambio productivo/política de medición justifica repetir una hora de perfiles largos solo para cambiar identidad Git.

## Smoke lectura remota: bloqueo inicial y recuperación verificada

El smoke pressure RustFS desde checkout limpio (64 filas, calentamiento 1s/medición 2s) **no pasó**. Preflight falló antes de crear fixture Fluss Docker: endpoint configurado `http://192.168.68.200:9000/fluss-lab` no era accesible.
Una comprobación health aparte de conexión tres segundos también venció. Inspección Docker/Kubernetes de solo lectura no encontró servicio RustFS local disponible en ese contexto; no cambió despliegue, credenciales ni configuración endpoint.

Ese intento permanece como preflight fallido en historial. Tras restaurar el usuario endpoint existente, se repitió el mismo comando smoke desde checkout Git limpio **`6f9749fabe1c490c90b776b221261e0f75cb11af`** y **pasó en 14,66s**. No cambiaron fuentes, credenciales laboratorio, ajustes endpoint ni límites prueba.

La ejecución completó 8 scans de calentamiento y 12 medidos durante 2,7s; pico RSS muestreado
110 MiB, VmHWM proceso 113 MiB, pico pool muestreado 4 MiB, máximo bytes temporales
100.320 y descargas remotas nativas **2.006.400 bytes**. Límites superiores SELECT
p50/p95/p99 fueron 114/704/704ms, sin desbordamiento de histograma. Bytes remotos no cero verifican ruta remota en vez de fallback solo local. Pasaron comprobaciones forma fila/valor, cancelación temprana, limpieza pool/temporales y eliminación prefijo. No quedan contenedores propios perfil y checkout probado siguió limpio.

El bloqueo externo se resolvió: cada comprobación checkout limpio planificada tiene resultado aprobado. Smoke corto de recuperación es evidencia funcional DEBUG; mediciones RELEASE largas acotadas siguen registradas por separado en `native-profile-plan.md`.
Aceptación se limita a esas configuraciones/límites semánticos documentados.

## Comandos de reproducción

Ejecutar desde checkout limpio de la fuente probada, con target externo escribible:

```sh
export CARGO_TARGET_DIR=/absolute/path/to/external/build-target
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --offline --locked
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo test --manifest-path clients/rust/Cargo.toml -p fluss-rs --offline --locked --lib
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo build -p fluss-datafusion --offline --locked --example native_query
FLUSS_NATIVE_QUERY_EXAMPLE="$CARGO_TARGET_DIR/debug/examples/native_query" DATAFUSION_POOL_MIB=16 DATAFUSION_TARGET_PARTITIONS=2 CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 uv run --no-project --env-file /absolute/path/to/lab/.env cargo test -p fluss-datafusion --offline --locked --test write_sql -- --ignored --test-threads=1
KUBECONFIG=/absolute/path/to/native-sni.kubeconfig CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 uv run --no-project --env-file /absolute/path/to/lab/.env cargo test -p fluss-datafusion --offline --locked --test live_log_sql -- --ignored --test-threads=1
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.6 CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p fluss-datafusion --offline --locked --test authorization --test write_pressure -- --ignored --test-threads=1
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.6 FLUSS_WRITE_WARMUP_SECS=1 FLUSS_WRITE_MEASURE_SECS=2 CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p fluss-datafusion --offline --locked --test write_profile -- --ignored --exact native_continuous_writer_profile
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.6 FLUSS_WRITE_CASE=log-contiguous FLUSS_PROFILE_LIVE_READ=1 FLUSS_WRITE_WARMUP_SECS=1 FLUSS_WRITE_MEASURE_SECS=2 CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p fluss-datafusion --offline --locked --test write_profile -- --ignored --exact native_continuous_writer_profile
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.6 FLUSS_PRESSURE_ROWS=64 FLUSS_PRESSURE_WARMUP_SECS=1 FLUSS_PRESSURE_MEASURE_SECS=2 CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 uv run --no-project --env-file /absolute/path/to/lab/.env cargo test -p fluss-datafusion --offline --locked --test remote_retention datafusion_resource_pressure_rustfs -- --ignored --exact --nocapture
```

Builds offline requieren dependencias registry ya descargadas. Compilar no requiere log local de medición, script exporter ni herramienta heaptrack.
