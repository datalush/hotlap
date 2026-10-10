# Controles nativos de asignación y copia

2026-10-05; build RELEASE/ocho jobs, afinidad CPUs 0–3. Herramientas Debian
heaptrack 1.5.0 se descargaron/extrajeron sin privilegios en `/tmp/opencode`; no
se añadió instalación de sistema, dependencia de aplicación ni asignador conector.
Las trazas son controles breves separados de perfiles sostenidos.

El harness controlado está versionado. Fingerprints de artefactos y trazas
pre-commit conservan el alcance original de medición.

Cada control escribe 1.792 filas únicas, 128 filas/lote y valores de 4 KiB,
calentamiento de un segundo y medición de dos. Confirmaciones, contenido final y
limpieza EOF/cancelación/recursos pasaron. Totales incluyen preparación, runtime/
collector de prueba, escrituras y lectura posterior; no son coste puro por fila
del encoder ni asignaciones solo de etapa medida.

## Particiones contiguas no implican buckets contiguos

En tablas old2/new3, agrupar filas por partición aún deja intercaladas filas de
buckets distribuidos por hash. Ambos layouts pueden requerir gathers nativos.
Controles log iniciales observaron 93.094 y 93.021 llamadas de asignación total;
esa pequeña diferencia no demuestra menos copias.

Para aislar slice vs take, el perfil incluye control **un bucket por partición
control** (`FLUSS_WRITE_SINGLE_BUCKET_CONTROL=1`). It uses normal client routing
y configuración normales; el profiler no replica hash/routing. Entrada contigua
puede usar slices por partición/bucket; entrada intercalada requiere gather. La
matriz sostenida old2/new3 no cambia.

| One-bucket control | Contiguous | Interleaved |
| --- | ---: | ---: |
| Complete-process allocation calls | 93,144 | 94,047 |
| Sampled retained Arrow backing peak | 526,976 B | 1,053,392 B |
| Sampled admitted encoded peak | 1,089,380 B | 2,132,608 B |
| Heaptrack global peak heap | 2.87 MB | 3.37 MB |

La traza intercalada contiene la pila nativa real
`FlussWriter::enqueue → AppendWriter::append_arrow_batch_with_retainer →
select_rows → arrow_select::take`. Its filtered report includes 168 allocation
llamadas atribuidas a pilas `take_bytes` y 56 a asignaciones box de `take_impl`.
La traza contigua no tiene pilas `arrow_select::take`, pero incluye asignaciones
IPC/esquema/scratch. La traza release conserva símbolos Rust v0 y frames padre
`select_rows`. Esto observa directamente materialización, no la infiere de RSS.
No toda diferencia de asignaciones del proceso se atribuye al gather; compartir
slices no promete codec sin asignaciones.

Heaptrack informa ~154 KB pendientes al salir, incluido estado global/test runtime.
No demuestra que toda asignación pendiente sea fuga; reserva pool cero tampoco
implica heap global cero. Limpieza de buffers source/sink propios se verifica aparte.

## Controles KV y materialización Arrow selectiva

Controles KV old2/new3 observaron **63.645** llamadas contiguas y **63.089**
intercaladas por proceso completo, ambos verificando mismo conteo/valores. Ruta KV
nativa sigue codificando filas/claves; no se afirma protocolo columnar KV ni cero
asignaciones.

En datos contiguos, todas-columnas/ID/COUNT usaron **8.203.632 / 41.920 / 0 bytes
Arrow decodificados**, cada uno recibió **7.369.977 bytes cuerpo RPC**. Todas las
columnas intercaladas usaron 8.138.096 bytes decodificados; ID y COUNT tuvieron
igual materialización pequeña/cero y tráfico de páginas crudas sin cambio. Cargos
de respaldo incluyen capacidades, no bytes extra de red. Métricas ScanKv exponen
tráfico mediante instrumentación RPC existente; trazas y admisión Arrow son métricas
separadas.

## Reproducción

Control slice (cambie el caso a `log-interleaved` para gather):

```sh
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.6 FLUSS_WRITE_CASE=log-contiguous FLUSS_WRITE_SINGLE_BUCKET_CONTROL=1 FLUSS_WRITE_WARMUP_SECS=1 FLUSS_WRITE_MEASURE_SECS=2 taskset -c 0-3 /tmp/opencode/heaptrack-tools/usr/bin/heaptrack --record-only -o /tmp/opencode/heap-log-slice target/release/deps/write_profile-c583ab388689dacc --ignored --exact native_continuous_writer_profile --nocapture
/tmp/opencode/heaptrack-tools/usr/bin/heaptrack_print -f /tmp/opencode/heap-log-slice.zst --filter-bt-function arrow_select --print-peaks 0 --print-temporary 0 --peak-limit 3 --sub-peak-limit 1
```

Use la ruta del ejecutable emitido por Cargo si difiere su fingerprint.
Artefactos: `/tmp/opencode/heap-log-{slice,take,contiguous,interleaved}.zst` y
`/tmp/opencode/heap-kv-{contiguous,interleaved}.zst`. APIs/formas de datos nativas y
la base del árbol de trabajo se registran en `native-profile-plan.md`; commit final
y reproducción desde checkout limpio son aspectos separados.
