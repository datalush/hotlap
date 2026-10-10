# Observaciones y métricas nativas de escritura

`FlussLogTable::subscribe_writes()` y `FlussKvTable::subscribe_writes()` devuelven
el receptor Tokio broadcast estándar de `FlussWriteProgress`. Suscríbase antes de
la ejecución. Capacidad: **128 eventos por provider**, compartida por sus escrituras
planificadas; providers clonados comparten canal. Cada ejecución física recibe un
ID distinto local al proceso, incluso al reejecutar el mismo plan o ejecutar consultas
concurrentes.

Esto informa el conocimiento de la aplicación sobre operaciones append/upsert/delete/MERGE.
No es checkpoint del motor, registro persistente de trabajos, aislamiento transaccional,
entrega exactly-once ni contabilidad de filas netas modificadas.

## Eventos y resumen final

| Evento | Significado |
| --- | --- |
| `Initialized` | ID de ejecución, ruta/tabla/esquema destino, operación, política ACK declarada y presupuestos de opciones writer; se emite antes de input/preparación |
| `BatchReceived` | Se observó lote de entrada no vacío, con ID secuencial y operaciones acumuladas recibidas/pendientes; la admisión aún puede rechazarlo |
| `BatchOutcome::Confirmed` | `flush()` nativo del lote completo terminó con éxito según modo ACK declarado; se publica antes de esperar más input/EOF |
| `BatchOutcome::RejectedBeforeEnqueue` | Metadatos, validación, cuota u otra preparación rechazó lote recibido antes de iniciar worker enqueue |
| `BatchOutcome::Uncertain` | Se intentó enqueue, pero no se confirmó el lote completo; pudieron aplicarse algunas, todas o ninguna operación |
| `Terminated(FlussWriteSummary)` | Snapshot acumulado completo de operaciones recibidas y terminación, incluida fase y si se observó EOF fuente |

`FlussWriteCounts` separa `received`, `confirmed`, `rejected_before_enqueue`,
`uncertain` y `pending`. Durante ejecución forman una partición del prefijo recibido;
al terminar, pending es cero. `uncertain` es **límite superior conservador de
operaciones quizá aplicadas sin confirmar el lote entero**, no prueba de que fallaran
o siquiera se enviaran. Filas fuente no leídas no forman parte de esos conteos.

El cliente nativo expone resultado agregado de flush para esta ruta. Enqueue puede
enviar algunos grupos bucket antes de fallar; flush fallido puede contener grupos ACK
y desconocidos. El conector no infiere división por fila del error ni de reintentos.
Conserva ACK completos de lotes previos y clasifica como incierto el lote actual
intentado. No se introducen handles por fila ni historial ilimitado de lotes retenidos.

El **estado de ejecución** fallida/cancelada es distinto del conocimiento de operaciones:

- ACK de lote 1 seguido de timeout/cancelación en lote 2 conserva lote 1 como confirmado.
- Error fuente tras ACK, mientras espera más input, no deja lote actual incierto.
- Rechazo metadatos/validación/cuota antes de enqueue significa que no se envió.
- Error limpieza tras EOF/ACK conserva operaciones confirmadas e `input_exhausted=true`.
- Cancelación antes de recibir input puede informar cero operaciones recibidas.
- Fallos planificación ocurren antes de ejecución sink y no producen inicialización.
- Plan DELETE/UPDATE optimizado y probado vacío devuelve conteo SQL cero sin invocar
  sink; por tanto tampoco emite observaciones Fluss. No se infiere ejecución faltante
  a partir de ese no-op upstream.

Política ACK: `FlussWriteAck::{All, Leader}`. `Leader` corresponde a `writer_acks=1`
y no ofrece la misma garantía de réplica que `all/-1`. La validación existente rechaza
modos no admitidos; la inicialización no declara política soportada en esos intentos.

Confirmación es hecho histórico de ACK, no prueba de checkpoint servidor ni flush a
disco. Crash inmediato de fixture `.6` de réplica única perdió prefijo recién ACKed;
prueba recuperación lo conserva tras ventana checkpoint servidor de seis segundos.
Ver [verificación de fallos nativos](native-failure-verification.md) para límite
observado y perfil exacto.

## Compatibilidad SQL y errores

El `count` final DataFusion sigue contando operaciones confirmadas, aparece solo tras
EOF y limpieza exitosa, y no se devuelve como conteo parcial exitoso ante fallo.
INSERT continuo emite confirmaciones mientras el conteo SQL sigue pendiente. Upserts
KV repetidos pueden contar dos veces con una sola clave final; DELETE cuenta operaciones
ACK de claves seleccionadas; MERGE cuenta acciones modificadoras, no filas join/no-op.

Observador es independiente de ejecución SQL normal. Errores originales DataFusion/
Fluss, fases tipadas timeout write y sus cadenas de origen no cambian. Observaciones
no llevan buffers fila, texto SQL, payload error ni credenciales. Aplicación puede
conservar error nativo junto al snapshot terminal estructurado.

## Pérdida y conciliación

Trate `RecvError::Lagged` / `TryRecvError::Lagged` como pérdida de detalle por lote.
Nunca continúe leyendo en silencio y declare historial completo. Snapshot terminal
posterior aporta totales acumulados, pero no recupera eventos individuales perdidos.
Si falta evento terminal/inicialización, observador no puede inferir éxito por silencio
ni por receptor cerrado/descartado. No combine IDs de ejecuciones distintas.

Para conciliar resultado incierto, motor necesita identidad destino, política ACK,
linaje de input/fuente propio, prefijo confirmado, límite superior del lote intentado
y comprobaciones de datos almacenados adecuadas a append vs upsert/delete PK. Ni
replay del trabajo ni reconexión borran requests posiblemente aplicadas. Este conector
no retiene claves/filas input para conciliar ni reintenta automáticamente trabajo.
IDs de ejecución son diagnósticos locales al proceso, no tokens de reanudación durable.

## Métricas nativas

Sink implementa `DataSink::metrics` de DataFusion; `DataSinkExec` estándar las expone
en métricas de plan/EXPLAIN ANALYZE. Un conjunto fijo se crea por sink planificado.
Reejecuciones acumulan contadores de ese plan; ejecuciones concurrentes suman/restan
gauges compartidos. IDs de ejecución están en observaciones, no labels de métricas,
por lo que reejecuciones no aumentan cardinalidad.

- Contadores de operaciones: `fluss_write_received_operations`,
  `fluss_write_confirmed_operations`, `fluss_write_rejected_before_enqueue_operations`,
  `fluss_write_uncertain_operations`, `fluss_write_confirmed_batches`.
- Contadores/gauges ejecución: `fluss_write_failed_executions`,
  `fluss_write_cancelled_executions`, `fluss_write_active_executions`,
  `fluss_write_pending_operations`.
- Gauges de vida de owners: `fluss_write_retained_arrow_bytes`, `fluss_write_encoded_bytes`,
  `fluss_write_transport_bytes`, `fluss_write_routing_metadata_bytes`,
  `fluss_write_routing_scratch_bytes`, `fluss_write_kv_scratch_bytes`,
  `fluss_write_merge_key_bytes`.
- Tiempos: `fluss_write_preparation_time`, `fluss_write_metadata_time`,
  `fluss_write_input_wait_time`, `fluss_write_enqueue_time`, `fluss_write_ack_time`,
  `fluss_write_cleanup_time`.

Gauges bytes siguen reservas existentes de buffer/Bytes/frame/worker a través de owners
reales. Una ejecución cancelada puede estar inactiva aunque un worker/frame termine de
retener bytes; siguen visibles hasta liberar owners. Scratch routing/KV permanece
cobrado legítimamente durante idle. Son valores de admisión cooperativa, con estimación/
solapamiento conservadores descritos en [presión de escritura](write-pressure-verification.md),
no bytes únicos asignados, slots cola nativa ni RSS. Política de admisión pool sigue
perteneciendo al pool DataFusion suministrado; guards métricas no crean otro pool.

## Verificación registrada

Pruebas core cubren cancelación rechazada vs intentada, conservación de ACK tras
cancelación idle/error limpieza, lag detectable con totales terminales, IDs distintos
y cardinalidad fija tras 200 ejecuciones. Matriz Docker real log/KV también observa
ACK antes EOF, error fuente tras ACK, incertidumbre lote completo con ACK bloqueado y
cancelación saturada, rechazo cuota previo a enqueue, terminación EOF y métricas/bytes
owner del sink nativo. Conteos de regresión INSERT/DELETE/MERGE siguen siendo contrato.
Aceptación conjunta streaming y aceptación final de fallos/perfiles/motor consumidor
se evalúan por separado.

Evidencia registrada: **29 pruebas core, matriz Docker completa log/KV y cuatro
regresiones SQL native-sni**. Clippy core all-targets/all-features `-D warnings`,
formato paquete y `git diff --check` pasaron. El comando combinado excedió su límite
externo durante SQL tras pasar las otras suites; SQL se repitió aparte y pasaron las
cuatro. Se eliminó la pareja exacta de tablas que dejó ese timeout; no quedan fixtures
Docker propios de presión.
