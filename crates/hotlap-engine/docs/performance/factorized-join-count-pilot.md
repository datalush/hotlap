# Protocolo del piloto de COUNT factorizado

Este ensayo compara tres rutas del kernel para un `COUNT(*)` agrupado por una
clave de un join equijoin interno: `reference` (Join seguido de GroupAggregate),
`candidate` (kernel factorized) y `manual` (preagregación de cada lado, join y
producto explícito). No mide SQL, el runtime durable, Flink, checkpoints ni
recuperación. El candidato sólo existe en compilación de pruebas.

## Requisitos y ejecución

Use el checkout aprobado, anote `git rev-parse HEAD`, sistema operativo,
arquitectura, CPU, memoria y versión de Rust. Compile una sola vez en modo
release con `CARGO_BUILD_JOBS=1 RUST_TEST_THREADS=1 cargo test --release -p
hotlap-engine --lib --no-run`. Luego ejecute, secuencialmente, cada combinación
de `HOTLAP_FACTOR_STRATEGY=reference|candidate|manual`,
`HOTLAP_FACTOR_CASE=distinct|duplicates|low|mixed` y
`HOTLAP_FACTOR_REP=0..4`:

```sh
CARGO_BUILD_JOBS=1 RUST_TEST_THREADS=1 \
HOTLAP_FACTOR_STRATEGY=candidate HOTLAP_FACTOR_CASE=distinct HOTLAP_FACTOR_REP=0 \
cargo test --release -p hotlap-engine --lib \
  arrange::factorized_join_count::benchmarks::measure -- \
  --exact --ignored --nocapture --test-threads=1
```

Conserve la salida y el código de salida de cada proceso por separado. No
seleccione sólo la repetición más rápida. La prueba imprime una fila CSV por
proceso; el resumen y los logs de resultados pertenecen al informe externo,
no a este protocolo. La línea CSV puede llevar el prefijo de progreso del
test harness; al extraerla, elimine el prefijo sin alterar la fila.

## Datos y estado

Los dos lados tienen schema fijo `(k: nullable Int64, payload: nullable Int64)`;
la salida es `(k, count: nullable Int64)` con pesos de Z-set. `distinct` tiene
10.000 payloads izquierdos distintos de peso 1 y una fila derecha `(0,0,+1)`.
El valor de count está en el payload de salida; el peso Z-set del grupo es 1.
`duplicates` usa una fila izquierda repetida con multiplicidad 10.000 (no diez
mil payloads) y la misma fila derecha. Ambos alternan la derecha empezando con
`-1`; el seed aporta el count 10.000, el primer paso deja el grupo ausente y el
siguiente lo restaura. `low` inicia 1.000 claves con una fila por lado y reemplaza payload
izquierdo en cada clave una vez. `mixed` inicia 1.000 filas izquierdas y ocho
derechas en una clave caliente; cada paso reemplaza una fila de ambos lados.
Los primeros 40 pasos son calentamiento; se miden los 400 siguientes. Los
conteos analíticos esperados son 10.000, 10.000, 1 por clave y 8.000,
respectivamente. Las multiplicidades y fronteras exactas están fijadas en la
prueba de construcción de cargas.

Cada proceso genera las entradas Arrow una vez antes de construir una sola
estrategia y conserva los mismos batches durante seed, calentamiento y medición.
La preparación queda fuera de `setup_ns`, que mide la construcción del operador;
`seed_ns` informa la aplicación del seed. Los 40 pasos de calentamiento
actualizan la misma instancia y el estado resultante pasa directamente a los 400
pasos medidos. El reloj por paso empieza antes de `apply` y termina después del
fold/consolidación del delta de salida; excluye generación, validaciones exactas,
oráculo e impresión. Se informan nanosegundos acumulados de pasos medidos y
p50/p99 por nearest-rank (`ceil(p*N)-1`), no una media global. El throughput de
filas de entrada por segundo se deriva de `input_rows / (total_ns / 1e9)`.
`input_rows`
cuenta los registros de ambos deltas de entrada, incluidas retracciones.

## Memoria y límites

Cada proceso ejecuta una sola estrategia. `vmhwm_kib` procede de `VmHWM` en
`/proc/self/status`; si no está disponible, se informa `NA`, nunca cero. Es el
pico del proceso de prueba e incluye fixtures, seed y estado retenido. No debe
interpretarse como tamaño lógico del estado ni como memoria end-to-end.

El dominio medido es deliberadamente reducido: claves y payloads Int64 y un
COUNT por clave. No evalúa otros agregados, DISTINCT, claves compuestas,
filtros, ventanas, ingestión, snapshots, persistencia o recuperación. Las
conclusiones sólo caracterizan estas cargas y este kernel; no implican ventaja
general, aceleración 10x ni soporte durable.
