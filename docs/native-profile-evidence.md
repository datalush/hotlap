# Evidencia de perfiles nativos

Registro histórico de resultados de perfiles RELEASE de lectura y escritura. Los
criterios y comandos reproducibles se mantienen en
[plan de perfiles nativos](native-profile-plan.md). La proveniencia de fuente y
artefactos se conserva como dato histórico, no como requisito de compilación.

## Resultado RELEASE corto — aprobado

Una ejecución completó en **162,75 s**, con 56 scans de calentamiento y 128 scans
medidos durante **120,3 s**. Cada scan es un SELECT completo; la mezcla incluye
64 consultas log y 64 KV. Tasa mixta aproximada **1,06 scans/s**, incluida pausa
intencional de 1 ms del consumidor por lote. No es techo de throughput irrestricto
ni percentil de latencia por consulta.

| Recurso | Resultado observado |
| --- | ---: |
| RSS tras calentamiento | 104 MiB |
| Pico RSS muestreado | 116 MiB |
| VmHWM del proceso | 118 MiB |
| RSS final | 80 MiB |
| Pico reserva pool | 32 MiB |
| Pico temporales scanner | 414.912 bytes |
| Contador descarga remota nativa | 1.118.480.288 bytes |

Pasaron las comprobaciones de filas/duplicados/formato de valores, cancelación
temprana y bytes propios pool/temporales a cero en cada ola. El contador remoto
nativo cubre toda la carga, incluido calentamiento/preparación/cancelación, no solo
scans completos medidos. Limpieza del prefijo RustFS exitosa; no quedan contenedores
propios `datafusion-pressure`. Fixture usa nombre único y limpia explícitamente
servicio/prefijo incluso ante error o panic.

El HEAD base fue `adc4f70f760904e795a9cd81a7ce728581286f55`; SHA256 del diff
seleccionado de código nativo/lock/perfil del árbol de trabajo:
`086b54bf816c84b8fc889b4a76f7ebe4f571b0b6c6ad90966c223d403252f0d6`:

```sh
git diff -- Cargo.toml Cargo.lock clients/rust/Cargo.toml clients/rust/Cargo.lock clients/rust/crates/fluss/src clients/rust/crates/fluss-test-cluster/src crates/fluss-datafusion/src crates/fluss-datafusion/tests/remote_retention.rs | sha256sum
```

La compilación inicial de dependencias release tomó 7m20s, seguida por recompilación
de 44,54 s del harness actualizado. El tiempo de build queda fuera de medición.
Este fingerprint identifica un árbol de trabajo, no un commit autorizado ni una
reproducción desde fuente limpia. Log/exit status se guardaron fuera del repo en
`/tmp/opencode/native-profile-b2b6a64e2e504276942ee3a79263d6c4.{log,json}`;
código de salida cero. Perfil DEBUG histórico y esta ejecución RELEASE más corta
no aíslan una mejora atribuible solo al conector.

## INSERT continuo sostenido — aprobado

Los cuatro casos RELEASE completaron en **1.461,30 s** total, secuencialmente en
CPUs 0–3, con etapas fijadas 60/300 s y routing old2/new3. Cada etapa confirmó
**153.728 filas en 300,01 s** (~512,4 filas/s, incluido lote límite). Cada tabla
final tuvo **184.576** filas verificadas, incluido calentamiento. Casos contiguos
terminaron en EOF; intercalados cancelaron en idle tras ACK. Pasaron confirmaciones,
datos, incertidumbre cero y comprobaciones pool.

| Caso | Confirmación p50/p95/p99 (ms) | CPU medido (s) | RSS final (B) | HWM proceso (B) | Pico muestreado pool (B) |
| --- | --- | ---: | ---: | ---: | ---: |
| Log partition-contiguous | 10 / 13 / 14 | 6.06 | 67,391,488 | 67,391,488 | 3,220,116 |
| Log interleaved | 10 / 13 / 16 | 6.39 | 74,473,472 | 77,553,664 | 3,220,116 |
| KV partition-contiguous | 13 / 19 / 23 | 6.10 | 81,166,336 | 84,316,160 | 4,510,184 |
| KV interleaved | 12 / 17 / 19 | 5.90 | 86,011,904 | 86,011,904 | 4,246,896 |

HWM es acumulativo en el proceso serial; picos muestreados no abarcan todas las
asignaciones. Percentiles ms son límites superiores del histograma. Tiempos ACK/
enqueue acumulados nativos aparecen aparte en log; confirmación también incluye
preparación/entrada/codificación. Disponibilidad de buffer nativo nunca bajó de
786.432 B y no hubo picos de hilos en espera durante medición. No se afirma capacidad
de buffer saturado; evidencia de fallos cubre ese caso. Algunos gauges transporte
muestrearon cero al perder frames cortos, no por ausencia de asignaciones.

Bytes de cuerpo RPC enviados: **7.095.149 / 7.150.829** en los dos casos log,
**632.940.211** por caso KV. ACK/respuestas recibidas correspondientes: 57.923–
57.929 bytes. Diferencias de compresión/framing/protocolo impiden tratarlos como
ratios de throughput Arrow decodificado. Reservas pool de fuente son contabilidad
cooperativa, no bytes únicos de heap ni RSS server/Docker.

La lectura KV contigua de 184.576 filas materializó **774.635.752 / 3.047.584 / 0**
bytes Arrow decodificados para todas las columnas/ID/COUNT, con **759.272.507 bytes
RPC** por consulta. Lectura intercalada: **774.942.608 / 3.043.392 / 0** bytes Arrow
y **759.272.458 bytes RPC** por consulta. Se verificaron valores/regiones/IDs; CPU
para todo/ID/COUNT fue 0,80/0,37/0,34 s y 0,73/0,36/0,31 s respectivamente. Menor
materialización Arrow no poda páginas KV nativas enviadas por protocolo.

El log de la suite serial es
`/tmp/opencode/native-profile-05ef9282f8b64afb9bc4f82a369a5ea8.log`. La etapa
writer terminó con código cero; la etapa lectora larga aparece después. Controles
de asignación/slice/take están en [perfiles de asignación nativa](native-allocation-profile.md).


### Medición de fuente nativa continua

La carga finita de scans repetidos difiere de lectura no acotada. Caso separado
`FLUSS_PROFILE_LIVE_READ=1` lee concurrentemente el log real con
`FlussLogTable::open_with_options(LogReadOptions::default())`, mientras INSERT
continuo existente produce datos old2/new3. Usa mismo pool llamador 64 MiB, pausa
de 1 ms tras cada batch de lectura y seguimiento de IDs de tamaño fijo (200.000
slots; duración/tasa limitadas por ese tamaño). Calentamiento/medición: 60/300 s.
Requiere throughput lector ≥90% de 512 filas/s dosificadas, cada ID/región/valor
confirmado exactamente una vez, prefijo completo dentro de 15 s tras productor,
cancelación explícita de lector y reservas propias cero. Aplican también criterios
RSS/pool/confirmación del writer. Bytes RPC/CPU incluyen lector y writer, no solo ACK.

La ejecución RELEASE pasó en **365,57 s**. En 300,010 s de medición, lector nativo
no acotado observó **153.728 filas** (~512,4 filas/s) y writer confirmó igual conteo.
Verificó **184.576** IDs/regiones/valores completos incluido calentamiento; luego
canceló explícitamente y liberó todos los leases. HWM proceso **69.865.472 B**,
pico pool compartido **3.220.116 B**, CPU combinado **7,26 s**, confirmación p50/p95/
p99 **10/13/14 ms**, overflow cero. Bytes RPC lector/writer: 7.164.379 enviados /
6.968.742 recibidos. Es lectura continua nativa de datos Fluss entrantes, no
inferencia desde SELECT finito. Log/estado:
`/tmp/opencode/native-profile-a7922664c0e546e2b56529eb2af94125.{log,json}`.

El comando de reproducción queda en el plan de perfiles; el log anterior identifica
la ejecución histórica.

### Lectura RELEASE sostenida completa — aprobada

La etapa lectora terminó con exit cero en **2.118,91 s**: calentamiento de 5 min
(324 scans completos), luego **1.796 scans medidos en 1.803,4 s**. Mezcla: 898
consultas log y 898 KV, cancelación temprana repetida y pool/bytes temporales cero
entre olas. Dataset/política: log decodificado 600 MiB / pool host 512 MiB, cuatro
SELECT concurrentes y CPUs 0–3.

| Recurso / latencia | Resultado |
| --- | ---: |
| RSS calentamiento | 77 MiB |
| Pico RSS / VmHWM proceso | 118 / 118 MiB |
| RSS final | 67 MiB |
| Pico pool muestreado | 35 MiB |
| Pico archivos temporales | 539.760 B |
| Descarga remota nativa, carga completa | 13.349.538.688 B |
| SELECT completo p50/p95/p99 (límites superiores) | 115 / 3.968 / 4.236 ms |
| Desbordamiento histograma latencia | 0 |

Pasaron criterios de memoria/disco/limpieza/datos y crecimiento RSS ≤256 MiB tras
calentamiento. Percentiles mezclan KV rápido de 64 filas y log grande de 4.800;
no son latencia solo-log ni ACK de protocolo. Tasa ~0,996 scans/s mixtos con pausa
intencional de consumidor lento, no techo irrestricto. Ambas etapas seriales salieron
cero; estado final junto al log; no quedan contenedores propios y limpieza prefijo
RustFS fue exitosa.

El baseline histórico DEBUG midió 112 scans en 1.854 s. Esta ejecución RELEASE
fue más rápida con sus ajustes, pero cambiaron juntos modo de build y ruta final;
no se atribuye la diferencia exclusivamente al conector ni a otro motor.

Los perfiles de ruta final cubren lectura sostenida 5+30 min, INSERT continuo log/KV,
routing de particiones mixtas y proyección KV ancha, con shapes y límites específicos.
La evidencia no es throughput máximo ni aceptación arbitraria de otro servidor.
Perfiles históricos 5+30 min y renovación STS real de 900 s siguen siendo bases
limitadas a su contexto. No se deriva paridad Flink, techo RSS universal ni garantía
exactly-once de estas mediciones.
