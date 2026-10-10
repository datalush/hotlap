# Semántica de lectura antes de los adaptadores del motor

Este documento describe la implementación actual. El contrato objetivo canónico
Rust-first y los requisitos explícitamente pendientes están en
[rust-contract.md](rust-contract.md); no todas sus decisiones están implementadas.

El cliente Rust de `clients/rust/crates/fluss` ya devuelve objetos Arrow
`RecordBatch`. Se reutiliza comportamiento del cliente/codec Rust nativo, no se
reimplementa. Integración FFI/Python queda fuera del alcance activo.

`TableScan::limit(n).create_bucket_batch_scanner(bucket)` devuelve como máximo
`n` filas **por bucket**. Sirve para previews explícitos, pero no demuestra una
vista completa actual de una tabla con clave primaria. Un scanner log devuelve
cambios, no el estado vigente de una tabla con PK. No exponga ninguno como
`read_table()` sin calificar ni como tabla SQL irrestricta.

El provider de **clave primaria** usa RPC `ScanKv` del servidor, no preview
limitado ni reconstrucción de changelog. El snapshot RocksDB del servidor aporta
cada fila vigente por bucket; upserts/borrados se reflejan allí. Continuaciones
reutilizan la misma sesión snapshot. No hacen falta archivos snapshot externos
ni combinación snapshot/log para este scan online. Una sesión fallida no inicia
otro snapshot en silencio; cancelar cierra sesiones conocidas cuando es posible,
y el servidor aplica TTL a apertura inicial interrumpida. Timeout de consulta
falla, no devuelve resultado parcial. Cada bucket abre snapshot al leerse por
primera vez; no hay snapshot transaccional entre buckets con escrituras
concurrentes ni snapshot retenido entre consultas.
Se admiten tablas KV particionadas y no particionadas. No se empujan filtros de
filas ni límite SQL global a `ScanKv`; DataFusion los evalúa exactamente. Las
proyecciones no vacías se materializan con decoder compacto selectivo y builder
Arrow después de leer registros. `COUNT(*)` solicita cero columnas y mantiene
conteos sin construir columnas Arrow de valores; páginas crudas y framing de
esquema/registro aún llegan. Valores lógicos no solicitados no se convierten ni
validan. Cambios de esquema y topología no particionada requieren replanificar;
layouts particionados se redescubren en cada ejecución.

La fuente log batch de solo append usa reader de offsets acotado del cliente; la
continua se suscribe al scanner de lotes no acotado. Ambas comparten proyección y
pruning. Pushdown solo poda lotes; el motor evalúa filtros exactos. Se traducen
comparaciones representables `Int32`/`Int64` solo en tablas con esquema inicial.
Los conjuncts pueden empujarse independientemente porque DataFusion conserva la
expresión completa. No se empujan `OR` ni conversiones numéricas con pérdida.
Configure `table.statistics.columns` antes de escribir a log para estadísticas
útiles de poda. Proyección, límites y particiones deben reflejar estas semánticas.

El provider log admite logs de solo append, incluidas tablas particionadas. Ambos
providers calculan streams físicos con conteo de buckets de tabla al planificar,
paralelismo objetivo DataFusion y cap positivo opcional del conector. Al ejecutar,
los buckets reales de cada partición seleccionada se distribuyen entre streams,
aunque difieran del default de tabla. Streams log comparten una captura de offsets
por ejecución.
Por defecto cada bucket inicia en su offset **retenido** más antiguo capturado
junto al offset más reciente; posición explícita puede usar offsets latest capturados
o un mapa completo de IDs tabla/partición/bucket a offsets inclusivos.
Mapeos incompletos/obsoletos e inicios fuera de rango fallan explícitamente.
Reusar plan físico con un `TaskContext` nuevo captura offsets nuevos. Antes de
suscribirse, la fuente comprueba que retención no haya avanzado más allá del
inicio; si lo hizo, consulta falla y no empieza en datos
más recientes. Respuesta server fuera de rango durante scan también falla. Lectura
acotada termina o falla explícitamente por timeout. Offsets se capturan por bucket,
no como snapshot transaccional entre buckets. En tablas con esquema inicial se
solicitan proyecciones SQL no vacías al scanner Fluss. Tras cambiar esquema, fuente
lee filas completas y proyecta localmente: lotes antiguos pueden carecer de campos
nuevos y server puede rechazar proyección/predicado sobre ellos. `COUNT(*)` de cero
columnas también obtiene filas completas y las elimina localmente. Filtros exactos
y límites SQL globales siguen siendo operaciones DataFusion.
Cuando se poda cada lote final, offset consumido del scanner avanza aunque no
produzca lotes Arrow. El reader acotado comprueba ese progreso y termina al
alcanzar cada offset de parada capturado; DataFusion hace polls breves y conserva
el timeout general.
Particiones que empiezan tras terminar otras conservan offsets compartidos.
Otra partición puede reintentar inicialización de offsets cancelada antes de
entregar filas. Offset ausente/inválido o fallo tardío de partición produce error,
no resultado completo. Consultas concurrentes requieren `TaskContext` distintos;
el mismo contexto en ejecuciones solapadas del plan no distingue consultas.
`EXPLAIN ANALYZE` informa bytes Arrow decodificados/de salida y pico de lote, no
bytes de red ni memoria de proceso. Métricas temporales miden captura de offsets
y espera de lotes; no son plazo end-to-end. `fluss_active_partition_streams`
registra vida de streams fuente, incluso fallidos/cancelados, y queda en cero al
terminar consulta. Gauge de pico no incluye buffers de operadores DataFusion aguas
arriba del scan.
En tablas particionadas, ejecución descubre nombres/IDs una vez y comparte lista
entre streams físicos. Consultas log capturan una vez por ejecución offsets de
parada de buckets; sesiones KV abren lazy por bucket. Particiones nuevas pertenecen
a siguiente ejecución. Si se elimina una partición en vuelo, sesión abierta puede
completar o scan pendiente fallar; no suponga aislamiento atómico DDL/lectura.
Para claves `Utf8`, igualdad simple con literal string (incluidos conjuncts bajo
`AND`) puede podar particiones; `OR`, casts y otras condiciones se evalúan
exactamente en DataFusion sin pruning. Cada partición aporta conteo buckets propio
para rangos, routing y asignación física. Cambiar default no reescribe particiones
antiguas; el plan redescubre layout en ejecución siguiente. Conteos ausentes o
inválidos hacen fallar scan, no omiten buckets. Cada consulta **batch** tiene
timeout finito.

`FlussLogTable::open` continúa batch para llamadores existentes;
`open_with_options(LogReadOptions::default())` usa streaming. Un scan streaming
no tiene deadline de fin de consulta: en idle espera registros, sin EOF. Deadlines
de operación/red del scanner siguen configurables. DataFusion ve
`Boundedness::Unbounded`, emisión incremental y sin orden global; sorts/agregados
globales no necesariamente terminan. Durante polling fuente verifica ID/esquema
tabla e IDs/conteos de particiones periódicamente; cambios fallan explícitamente,
no incorporan buckets desconocidos sin posiciones iniciales. Particiones nuevas
requieren otra ejecución. `subscribe_deliveries()` observa lotes de fuente con
tabla/partición/bucket, offset inicial/siguiente exclusivo e ID de ejecución.
Pruning servidor puede dejar huecos y SQL downstream filtrar lotes completos. No
son offsets precargados ni checkpoints procesados/confirmados de sink. Rezago del
observador es error.

`subscribe_progress()` añade asignación inicial por partición, incluidos buckets
vacíos, bajo ID de ejecución compartido; rangos streaming no tienen stop. Eventos
Offered mantienen contrato de entrega. Rangos Excluded avanzan solo hasta primer
lote aún en cola para bucket (y no más allá de stop finito), así completar fetch no
salta filas no ofrecidas. Rangos acotados completados siguen observables tras
unsubscribe del cliente; stream idle totalmente podado puede avanzar sin salida.
Cada partición ejecutada informa Completed/Failed/Cancelled; inicialización/terminal
ausente o broadcast Lagged vuelve incompleta la evidencia. Para asignación finita
completa, reúna inicialización de cada partición. LIMIT puede parar antes de que
todas corran: no invente offsets desconocidos. Reanudar requiere mapa completo
validado con igual semántica de selección; progreso ofrecido/excluido no significa
trabajo procesado ni confirmado.

`EXPLAIN ANALYZE` identifica scans log/KV, tabla y columnas proyectadas,
predicado batch log opcional y pruning de particiones. Conteos describen
particiones descubiertas/seleccionadas (una vez, no por stream físico,
independientemente de cuál empiece primero).
`fluss_buckets_assigned` cuenta pares partición/bucket seleccionados realmente y
asignados a streams ejecutados, incluidos rangos vacíos. KV cuenta sesiones
confirmadas por servidor y respuestas RPC `ScanKv` exitosas (incluidas páginas
vacías), no lotes Arrow salida. Latencia primera página incluye request y decode.
Proyección se identifica como `server` en logs de esquema inicial,
`client_evolved_schema` en logs evolucionados, `decoder` en KV proyectado,
`row_count_only` en COUNT KV o `full_rows_for_count` en scans log sin columnas.
No son mediciones de bytes de red ni de apertura snapshot solo servidor.

Cada fuente reserva buffers de respaldo Arrow decodificados en pool DataFusion
compartido. Reserva se asocia mediante API de propiedad de asignación custom de
Arrow, sin copiar buffers de valores/offsets/validity. Clones, slices/proyecciones
mantienen lease conservador del lote completo hasta liberar último buffer retenido,
incluso tras fin/cancelación de stream fuente. Operadores downstream que materializan
arrays nuevos contabilizan su propia salida/estado; estos leases cubren buffers
fuente retenidos, no toda asignación posible de resultados SQL.

Lotes log decodificados en reader batch/streaming se cobran en reserva de cola
separada tras polling y antes de ofrecer salida. Si falla admisión, fuente informa
error y libera su cola propia. Respuestas crudas, batches dentro de poll cliente
inacabado, descompresión y archivos remotos tienen límites de cliente aparte: no es
límite previo a decodificar ni techo RSS. El cap de 64 MiB del poll cliente es
blando. Scan con esquema evolucionado reserva respaldo decodificado completo antes
de proyectar; retener un buffer proyectado puede mantener conservadoramente cargo
del lote entero. Wrappers de array asignan metadatos; capacidad de buffer custom
refleja vista visible y lease cobra capacidades originales. Métricas bytes
decodificados/de salida pueden diferir de reservas pool.
Providers log/KV aceptan `with_max_retained_batch_bytes(bytes)`, 64 MiB por defecto,
como techo independiente por lote retenido. Exceso produce ResourcesExhausted nativo
incluso con pool ilimitado. Se comprueba después de decode.
`fluss_retained_source_buffer_bytes` mide leases vivos de respaldo, no solo último
pull; persiste mientras consumidores retengan buffers.

Los pulls de fuente nativos decodifican como máximo un lote mediante API limited
poll del cliente. Se quitó cola decodificada streaming del conector; reader batch
también solicita un lote, no poll masivo. API bulk del cliente sigue disponible
con cap de bytes blando. Poll batch no espera otro fetch después de consumir salida.
Ver [verificación de presión de lectura](read-pressure-verification.md) para
límites, cancelación y evidencia repetida de presión/remoto.

Particiones log/KV batch comparten deadline de ejecución de fuente, iniciado por
la primera partición ejecutada. Una partición tardía recibe el mismo deadline,
no timeout nuevo. Reejecución secuencial/contexto nuevo crea ámbito distinto. No
limita trabajo SQL downstream arbitrario. Expiración de fuente es
`FlussScanTimeout` inspeccionable en error External DataFusion; red/almacenamiento
del cliente preservan causa original. Idle streaming sigue sin límite de fin.
Inicialización/topología streaming usan timeout de operación remote-log de conexión;
poll añade su intervalo idle normal a ese plazo finito. `FlussOperationTimeout`
distingue ese fallo de deadline de finalización. Cambios locales identidad/esquema/
topología/retención exponen motivos `FlussReadInvalidated` vía External sin
reemplazar causas del protocolo.

El catálogo descubre nombres una vez; vuelva a cargarlo tras crear tablas.
Selecciona provider log o KV según metadatos de clave primaria.

La semántica de `INSERT INTO`, sus presupuestos, DELETE y MERGE se describe en
[semántica de escritura](write-semantics.md).
