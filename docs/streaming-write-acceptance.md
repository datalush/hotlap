# Aceptación de INSERT continuo nativo

Se usa el mismo sink Rust/DataFusion que INSERT finito, con entrada no acotada hacia
append log y upsert KV de fila completa. `RecordBatch` es unidad de procesamiento,
no requisito de consulta finita. DELETE selecciona KV finito y MERGE requiere
entrada finita. Checkpoints/reintentos/conciliación del motor son externos.

## Fuente Fluss real hacia destinos log/KV

`tests/write_sql.rs::streaming_source_routes_log_kv_confirms_sparse_batches_and_replays_explicitly`
crea fuente log aislada de un bucket y dos destinos particionados: `north` conserva
**2 buckets**; `west` se crea con **3 tras rescale**. Ejecuta dos sentencias INSERT
SELECT concurrentes con `id % 2 = 0`, usando provider streaming Fluss nativo y pool
DataFusion suministrado.

- Un append Arrow de 24 filas intercala particiones destino y claves bucket. Cada
  sink confirma 12 operaciones filtradas antes del EOF, con IDs de ejecución
  distintos. Se almacenan 12 filas, seis por destino north/west.
- Idle dura **1,3 s**, más que el allowance ACK de **1 s**; ambas sentencias siguen
  activas y no emiten conteo SQL final.
- Un singleton disperso lleva ACK acumulado a 13 sin llenar lote cliente ideal.
  Cancelar INSERT log mientras espera entrada informa confirmed13/uncertain0 y solo
  cierra ese writer privado.
- Otro singleton llega al peer KV activo y confirma14. Destino log queda en13;
  cancelar KV idle informa confirmed14/uncertain0.
- Reservas propias source/sink/cliente vuelven a cero dentro del plazo observado tras cancelar.

## El replay explícito no es exactly-once

Ejecuciones nuevas comienzan explícitamente en earliest de fuente, con IDs nuevos.
Ambos sinks confirman **14 operaciones**. Log append queda con **27 filas** (13
previas +14 replay); destino KV conserva **14 claves**. Conteo upsert no es conteo
neto de claves ni garantiza deduplicación de negocio. Ambas sentencias replay
requieren cancelación explícita porque su entrada es continua.

Para reiniciar con offsets explícitos, el motor debe aportar posiciones completas
y válidas por bucket y conservar conocimiento propio de procesamiento/checkpoint.
Progreso ofrecido, ACK e IDs write locales no son commits coordinados del motor.
Reproducir solicitudes inciertas puede duplicar filas append o sobrescribir PK; el
conector no reintenta sentencia ni concilia automáticamente.

## Matriz conjunta de aceptación y reutilización de evidencia

| Requisito | Evidencia verificada en ruta Rust final |
| --- | --- |
| Fuentes continuas reales, routing old/new de múltiples particiones/buckets | Prueba native-sni source→log/KV; contraparte finita compara pertenencia/orden/NULL de buckets con API nativa por filas |
| Confirmación pequeña/dispersa visible antes de EOF | Prueba fuente nativa, prueba SQL log continuo original y matriz Docker de ambos destinos |
| Idle sin finalización/conteo inventado | Idle real 1,3 s > ACK 1 s; idle Docker log/KV > ACK 2 s; resumen terminal/EOF probado aparte |
| Contrapresión ante productor rápido | Matriz Docker agota buffer cliente 64 KiB/objetivo 32 KiB con entrada 1 MiB y servidor pausado; luego cancelación cooperativa y recuperación pool. Sigue pasando regresión finita 4 MiB/2 MiB |
| Propiedad buffers source/sink/cliente | Pruebas de presión lectura/consumidor retenido y gauges escritura/propiedad/admisión pool; pulls fuente de un lote y leases Arrow sin cambios |
| Fallo tras confirmaciones | Error fuente Docker log/KV tras ACK conserva confirmed1; ACK bloqueado posterior causa fallo SQL, confirmed1/uncertain1 y causa original |
| Cancelación esperando entrada | Ejecuciones source log/KV reales y replay explícito paran sin borrar ACK |
| Cancelación con buffer lleno | Matriz Docker ambos destinos, confirmed1/uncertain256; guards source/worker/frame/routing liberados dentro del plazo observado |
| Cancelación esperando ACK | Caso singleton explícito ambos destinos verifica etapa terminal `Ack`, estado cancelado, confirmed1/uncertain1 y pool liberado; separado de timeout/admisión buffer |
| Ejecuciones independientes | IDs write distintos en misma fuente/contexto real; cancelar log no detiene entrega KV; peer Docker privado sobrevive a cancelación con destino pausado |
| Replay/duplicados/incertidumbre | Replay explícito earliest comprueba log27/KV14 con 14 operaciones ACK por ejecución; documenta incertidumbre máxima por lote sin rollback/reinicio automático |

La fuente Docker usa StreamingTable DataFusion nativa y misma ejecución conector,
con GreedyMemoryPool real instrumentado para ubicar fallos determinísticamente. No
sustituye fuente Fluss de producción. Combinar esos fallos controlados con entrega/
routing de fuente Fluss real evita otro transporte, writer, scheduler, política de
pool o framework de eventos genérico.

## Límites de recursos/observación

Se mantienen límites existentes de retención de entrada, colas nativas y pool
compartido elegido por llamador. Admisión decode/gather/frame Arrow no limita
asignador global/RSS. Buffers/operadores retenidos externamente siguen cobrados tras
cancelación hasta liberar owners. Envío/drenado de frame parcial nativo tiene límite
propio de 30 s; recuperar frames pequeños ≤3 s no prueba que cada kernel/socket se
pueda terminar forzosamente en 3 s.

Observadores broadcast son acotados; Lagged/terminal ausente implica historial
incompleto. ACK previos siguen conocidos; lote actual intentado queda incierto
conservadoramente. `count` aparece solo tras EOF y limpieza exitosa. No queda writer
streaming transitorio: log usa ruta Arrow cliente, KV codificación nativa por filas;
ambos mediante DataSinkExec.

See [write-observation-contract.md](write-observation-contract.md),
[write-pressure-verification.md](write-pressure-verification.md) and
[read-pressure-verification.md](read-pressure-verification.md) for exact boundaries.

## Verificación registrada

Suite SQL native-sni final: **8 pruebas aprobadas**, incluida nueva prueba con fuente
real. Matriz Docker propia pasó con cancelación ACK explícita añadida a ambos destinos.
Clippy core all-targets/all-features `-D warnings`, formato de paquete y
`git diff --check` pasaron. DEBUG/ocho jobs; no incluye perfil benchmark RELEASE,
rebuild bindings ni reemplazo wheel. Matriz permisos/failover, perfiles sostenidos
y aceptación final Rust siguen siendo gates independientes.
