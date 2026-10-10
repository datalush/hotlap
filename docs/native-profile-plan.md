# Perfiles nativos de la ruta final

Los artefactos de medición conservan sus nombres originales y el contexto fuente registrado.

Los criterios de aceptación se fijaron antes de medir, el 2026-10-05. Perfiles
con RELEASE y ocho jobs de compilación; suites de corrección con DEBUG/ocho jobs.
Las ejecuciones midieron el árbol basado en `adc4f70f760904e795a9cd81a7ce728581286f55`;
los harnesses y la ruta de medición ScanKv se versionaron después en `19dca49`.
Son identidades históricas de fuente y proveniencia de pruebas; no se afirma que
cualquiera de esos commits registre por sí solo el resultado medido. Rutas de
artefactos y fingerprints de snapshots son ubicaciones de evidencia, no requisitos
de compilación. La reproducción Git limpia se informa por separado.

## Primera medición: carga existente de presión de lectura

Reutilice `remote_retention::datafusion_resource_pressure_rustfs`; su muestreador
comprueba RSS del proceso, reservas cooperativas del pool y bytes temporales del
scanner. Utilice la ruta nativa actual e imagen `.6`, prefijo RustFS único, afinidad de
cuatro CPU y sin otro benchmark concurrente.

- Datos: 4.800 filas log de 128 KiB (600 MiB decodificados, mayor al pool) y 64 KV
  (8 MiB). Sembrar con cliente nativo es preparación, no medición de throughput sink SQL.
- Pool: 512 MiB compartidos por cuatro SELECT concurrentes (dos log, dos KV), con
  dos particiones físicas de fuente por consulta.
- Cliente: chunks remotos 1 MiB, dos operaciones lectura/dos descargas y dos
  segmentos precargados/64 MiB por scanner.
- Duración inicial: calentamiento 30 s y medición 120 s; olas activas pueden
  extender tiempo. Perfil medido corto, no aceptación sostenida histórica 5+30 min.
- Presupuestos observados: RSS y VmHWM kernel ≤1,5 GiB; temporales scanner ≤2 GiB;
  bytes pool/temporales a cero tras ola/cancelación; IDs únicos completos, valores
  128 KiB y prefijos coincidentes en cada scan completo. No compara cada valor byte
  por byte.
- Informar scans/tiempo, RSS tras calentamiento/final/pico, reserva/temporales máximas
  y bytes remotos. Se requieren bytes remotos no nulos para clasificar evidencia
  de lectura remota. Harness corto no aplica crecimiento RSS ≤256 MiB del perfil
  completo tras calentamiento; informar diferencia observada.
- Hardware: Intel Core i9-13900H, 20 CPU lógicas, RAM 62 GiB; rustc/cargo 1.97.1,
  Linux x86_64. Afinidad ejecución CPUs 0–3; compilación ocho jobs sin ese límite.
  RSS corresponde al proceso de prueba, no server/Docker Fluss ni host entero.

```sh
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.6 FLUSS_PRESSURE_ROWS=4800 FLUSS_PRESSURE_WARMUP_SECS=30 FLUSS_PRESSURE_MEASURE_SECS=120 CARGO_BUILD_JOBS=8 uv run --no-project --env-file ../lab/.env taskset -c 0-3 cargo test -p fluss-datafusion --locked --release --test remote_retention datafusion_resource_pressure_rustfs -- --ignored --exact --nocapture
```

El resultado y las tablas de métricas de la ejecución corta se conservan en
[evidencia de perfiles nativos](native-profile-evidence.md).

## Criterios y comando de carga continua

`write_profile::native_continuous_writer_profile` fija la carga antes de ejecutar:
128 filas/lote, valores 4 KiB y máximo cuatro lotes/s (512 filas ofrecidas/s), una
confirmación pendiente, canal de entrada capacidad uno, particiones destino una,
pool host 64 MiB y buffer cliente nativo aparte de 2 MiB con lotes 256 KiB. Cada
caso log/KV usa particiones old2/new3, primero grupos contiguos y luego intercalados.
Calentamiento 60 s y medición
duración 300 s por caso. Casos contiguos terminan en EOF; intercalados cancelan en
idle tras último ACK. Se releen ID/región/valor completos confirmados; incertidumbre
y reservas propias del pool quedan en cero tras completar/cancelar.

Criterios fijados: al menos 90% de oferta dosificada (3,6 lotes/s), confirmación
p99 como máximo 1 s, sin overflow del histograma acotado de ms, RSS/HWM proceso
≤1,5 GiB, crecimiento RSS tras calentamiento ≤256 MiB y pico pool ≤64 MiB.
Informe percentiles de confirmación aparte de tiempo ACK/enqueue nativo, gauges de
owner/disponibilidad buffer/hilos espera, CPU proceso y bytes cuerpo RPC nativos.
Muestree solo etapa medida, no startup ni cierre EOF. Son cargas controladas, no
throughput máximo.

Lectura KV compara todas las columnas, solo ID y COUNT sobre mismos datos. Verifique
datos completos, menor materialización Arrow para ID y cero bytes Arrow decodificados
para COUNT, registrando aparte bytes reales de cuerpo RPC. ScanKv está en whitelist
existente de métricas RPC con labels fijos; antes esas
solicitudes no se reportaban y parecían cero bytes. Recorder facade conserva
contadores tras destruir contexto e ignora histogramas facade; no añade transporte,
política memoria ni pila de reintentos.

```sh
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.6 CARGO_BUILD_JOBS=8 taskset -c 0-3 cargo test -p fluss-datafusion --locked --release --test write_profile native_continuous_writer_profile -- --ignored --exact --nocapture
```

`FLUSS_WRITE_CASE` puede seleccionar `log-contiguous`, `log-interleaved`,
`kv-contiguous`, `kv-interleaved` para trazas de asignación separadas. Son controles instrumentados cortos, no resultados de
aceptación de rendimiento. Herramientas Heaptrack se extraen sin privilegios fuera
del repo; no se añade asignador/dependencia al conector. Perfil largo de lectura
registra latencias SELECT reales en histograma fijo (límite superior ms p50/p95/p99).

## Estado de mediciones

Las mediciones completas de lectura continua, perfiles sostenidos y resultados
numéricos se mantienen en [evidencia de perfiles nativos](native-profile-evidence.md).
La evidencia no implica una garantía universal de RSS ni exactly-once.
