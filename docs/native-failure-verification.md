# Permisos, fallos y recuperación nativos

Verificación del árbol de trabajo 2026-10-05. Builds funcionales DEBUG con
`CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0`. Resultados ejercitan cliente Fluss
nativo y providers/planificador/runtime DataFusion.

Los resultados son evidencia histórica del árbol indicado. Resultados de checkout
limpio se registran por separado.

## Permisos reales y causas preservadas

`tests/authorization.rs` crea fixture Docker SASL/ACL propio con nombre único y
autorización activa. Credenciales admin/reader/writer son exclusivas del fixture;
reader tiene permisos Describe/Read y writer además Write.

- SELECT del reader devuelve filas log/KV sembradas.
- INSERT log/KV, DELETE y MERGE del reader fallan con `AuthorizationException`
  nativa en cadena de errores DataFusion; se verifica con idempotencia writer
  nativa desactivada y activada.
- Ejecuciones KV fallidas publican resumen terminal Failed con cero operaciones
  confirmadas; ambas tablas conservan datos originales tras denegación.
- INSERT/MERGE/DELETE autorizados devuelven conteos esperados; KV final contiene
  exactamente `(1, 'authorized')` y `(3, 'new')`.
- Consulta completada/fallida libera reservas pool propias dentro de observación
  fixture de tres segundos.

Esto reveló tres defectos cliente: código ACL `0` se trataba como error; fallos
send/connect podían reemplazar error API por NetworkException; abortar asignación
writer-ID podía perder causa nativa por diagnóstico genérico broadcast. Parser ACL
acepta ahora códigos ausentes/cero, sender conserva errores API y flush accumulator
prioriza causa nativa almacenada. Regresión ACL cubre create/drop/filter con códigos
ausentes/cero/no-cero; fixture autorización cubre inicialización y envío writer.

## Pérdida real de socket y recuperación explícita

`tests/write_pressure.rs::real_writer_connection_loss_and_recovery` es independiente
de matriz saturación/timeout y usa fixture Docker propio de un tablet. Para
append log y upsert KV:

1. Confirma fila `9900` antes de EOF y da al prefijo ventana checkpoint server de seis segundos.
2. Retiene fila `9901` tras admitir metadatos; detiene tablet de inmediato (cierra
   sockets reales) y luego libera codificación. Conserva filesystem del contenedor.
3. Exige SQL fallido con causa External, resumen terminal Failed con una operación
   confirmada y otra conservadoramente incierta, y limpieza pool.
4. Reinicia mismo tablet y espera hasta 60 s recuperación líder/offset; conserva
   último offset/error observado en diagnóstico.
5. Usa conexión fresca y nueva ejecución explícita para escribir `9902`. SELECT final
   es `[9900, 9902]`; lote retenido no alcanzó server detenido. Reservas pool source/
   sink vuelven a cero al liberar resultados.

Matriz recuperación tiene deadline fixture 180 s y teardown panic/error. Matriz
presión separada conserva deadline 90 s y cubre agotamiento buffers, ACK bloqueado,
timeout, cancelación, aislamiento peers, error input tras ACK, invalidación topología/
esquema y carreras ACK parcial de DELETE/MERGE.

### ACK frente a durabilidad ante crash: límite observado

Primera versión detuvo server `.6` de una réplica justo tras ACK. Tras reinicio
reportó offset `0` durante 60 s; aumentar espera del líder no recuperó prefijo.
Con ventana checkpoint de seis segundos, log y KV conservaron prefijo y pasaron
comprobaciones finales. Default server `log.replica.high-watermark.checkpoint-interval`
es cinco segundos. Prueba concede esa ventana; no inspecciona checkpoint ni demuestra
garantía universal fsync/durabilidad.

Observaciones confirmadas siguen siendo hechos ACK históricos bajo política
declarada. No prueban checkpoint del motor ni persistencia inmediata ante crash.
Recuperación aquí reinicia mismo tablet, no promueve líder replicado. Durabilidad
de réplica/disco y conciliación requieren aceptación del perfil de servidor elegido;
esta fixture no prueba garantías mayores.

## Evidencia reutilizada y límites pendientes

- Casos presión/SQL cubren ACK perdido/bloqueado, intentos agotados, error de
  entrada tras lotes confirmados, cancelación escritura, aislamiento peers y
  comprobación final datos/recursos. Suite cliente cubre drenado acotado de frames cancelados.
- [Verificación de presión de lectura](read-pressure-verification.md) conserva
  cancelación fuente, propiedad buffers retenidos, invalidación y failover
  coordinador native-sni/consulta nueva. Fixes ACL/sender no cambiaron esos scanners.
- [Preparación para producción](production-readiness.md) conserva
  HTTP transitorio/permanente, retención y expiración/renovación STS. Esta pasada no
  no repitió perfil histórico STS de 900 s ni perfil sostenido de recursos.
- El alcance es esta matriz nativa registrada, con el límite de durabilidad anterior.
  Perfiles, cobertura motor y reproducción Git limpia conservan
  evidencia propia, sin ampliar semántica ACK.

## Verificación registrada

Ejecuciones finales en fuentes modificadas: autorización **1 aprobada (6,20 s)**;
presión/recuperación **2 aprobadas (121,68 s)**; SQL escritura native-sni **8
aprobadas (41,38 s)**; cliente **836 aprobadas, 2 ignoradas**; core conector **29
aprobadas**; planner genérico DELETE/UPDATE DataFusion **3 aprobadas**. Clippy
all-targets de raíz/cliente/test-cluster afectado pasó con `-D warnings`. No quedan
contenedores propios de autorización/presión/recuperación.

```sh
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.6 CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p fluss-datafusion --locked --test authorization -- --ignored --test-threads=1
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.6 CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p fluss-datafusion --locked --test write_pressure -- --ignored --test-threads=1
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 uv run --no-project --env-file ../lab/.env cargo test -p fluss-datafusion --locked --test write_sql -- --ignored --test-threads=1
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p fluss-datafusion --locked --lib --test delete_planner
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR="$PWD/target" cargo test --manifest-path clients/rust/Cargo.toml -p fluss-rs --locked --lib
```
