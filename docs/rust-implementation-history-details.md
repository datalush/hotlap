# Resoluciones y evidencia histórica de la auditoría Rust

Este registro conserva hechos del árbol auditado y resoluciones posteriores. La
auditoría original tuvo fecha **2026-10-04** y tomó como bases `d06288a` (MERGE),
`f66e90c` (INSERT/DELETE) y `89e111b` (batch/streaming), además de cambios FFI sin
commit encima. Las secciones que entonces describían un árbol de trabajo se
mantienen como evidencia de ese árbol; no se sustituyen por el HEAD actual.

El documento fuente original organizó hallazgos y código transitorio con etiquetas
internas. Esas referencias se retiraron; las decisiones y pruebas de cada tema
están preservadas en este detalle técnico y en
[hallazgos históricos](rust-implementation-findings.md).

## Resolución de planificación nativa — 2026-10-04

Los grafos auxiliares SELECT de DELETE y MERGE pasaron a `create_physical_plan`
de la sesión suministrada, recuperando su planner y optimización lógica. Una prueba
SQL con planner personalizado registra esos grafos. Se retiró el coalesce manual
motivado por FFI: INSERT desde MemTable de tres particiones verifica el enforcement
nativo, todas las filas y la reejecución del mismo plan físico; otras pruebas cubren
entrada continua, contrapresión y layouts antiguos/nuevos.

Se eliminó el contenedor duplicado de campos destino/sink: `FlussWriteTarget` es el
sink, y `plan_write` nombra la construcción común de INSERT/DELETE/MERGE. Se añadió
`capabilities.rs` como vista pequeña e inmutable derivada de modo/metadatos; no hay
registro de capacidades ni planner propio. Se conservó `InputPlan`: adapta una
fuente física MERGE al grafo lógico; una MemTable multipartición verifica su uso.
`FlussScanExec` conserva su responsabilidad distinta de métricas/presentación.
DELETE admite UDF y qualifiers de tabla nativos; alias MERGE funciona. DataFusion
55.1 rechaza alias del destino DELETE antes de planificación del provider y la
prueba registra esa limitación, sin workaround SQL.

Evidencia registrada para ese árbol: 15 pruebas unitarias Rust, cuatro integraciones
`write_sql` ignoradas ejecutadas en fixtures native-sni aislados y clippy
`fluss-datafusion --all-targets --all-features --locked -- -D warnings`. Builds
funcionales DEBUG/ocho jobs. Esto no prueba paridad con bindings FFI históricos ni
aceptación completa del runtime consumidor.

## Base de recursos nativos — árbol de trabajo, 2026-10-04

Las colas decodificadas de reader se cobraron tras poll completo con reserva de
fuente nativa; el cliente expuso `buffered_arrow_bytes()` sin depender de DataFusion.
Buffers raw, fetch sin terminar y protección pre-decode quedaron como recursos
transitorios/clientes separados: no se afirmó límite RSS estricto.

`resources.rs` adjuntó leases a buffers Arrow con owner de asignación custom,
conservando buffer inmutable original y reserva, sin copia payload ni tipo Array
alternativo. Proyección/slicing/retención downstream mantiene leases aunque se
libere estado/contexto fuente; lotes independientes se liberan independientemente.
Cargos por lote completo siguen siendo conservadores tras proyección; owners de
headers/arrays asignan metadatos y la capacidad visible de buffer custom puede no
ser igual a la capacidad original cobrada.

Plazos de lote log/KV compartieron contexto/generaciones de partición con capturas
existentes; particiones tardías no reinician el presupuesto. La limitación de
ejecuciones solapadas con el mismo `TaskContext` quedó explícita, sin scheduler ni
registro de identidad adicional. Errores timeout exponen `FlussScanTimeout` en
External DataFusion; otras clasificaciones y limpieza de writers bloqueados
permanecieron como límites separados. Se conservaron pool/TaskContext originales
y aislamiento de writer.

Evidencia de ese árbol: 18 pruebas unitarias (punteros de buffer, vidas nested/NULL/
slice, admisión fallida y generaciones deadline), integraciones log/KV con leases
retenidos/concurrencia y causas timeout de partición tardía, streaming append/cancel,
cuatro integraciones `write_sql` y seis pruebas existentes de `MemoryLimiter`.
DEBUG/ocho jobs; clippy core all-targets/all-features pasó. La última revisión
añadió errores en vez de panic para deadlines no representables y rechazo de ACK
menor de un milisegundo; conteo unitario final registrado: 20. ACK bloqueado,
colas y perfil de asignación completa eran verificaciones separadas.

## Lecturas Arrow selectivas

El decoder cliente pasó a recorrer campos compactados selectivamente y construir
solo columnas Arrow seleccionadas. Proyección, referencias duplicadas, alineación
de field IDs entre esquemas antiguos/nuevos y COUNT por filas KV se soportaron sin
workaround full-row/count del conector. Preview aplica límites sobre rangos de
registros antes de materializar. El inventario [Arrow read](arrow-read-inventory.md)
conserva límites antes/después, validación de valores no solicitados, evidencia de
buffers y materializaciones log/formato retenidas. No se eliminaron picos raw/decode
ni se sustituyeron perfiles de presión/throughput.

## Progreso de fuente y endurecimiento de lectura

El progreso log se separó en observaciones `LogProgress` acotadas sin cambiar
`LogDelivery`: rangos iniciales para cada partición física (incluidos buckets
vacíos), lotes ofrecidos, rangos excluidos y estado terminal. Identidad se asigna
antes de la primera salida y se comparte entre particiones; no transporta errores,
credenciales ni checkpoint de motor. Metadatos read-only cliente exponen offsets
fetch, bases de lotes en cola y límites de parada. El conector limita avance
reanudable al primer lote aún no ofrecido y al final finito, incluso en buckets
desuscritos después del fetch. Errores `Lagged` estándar exigen rechazar evidencia
incompleta; mapas explícitos de offsets conservan validación. Reuso solapado del
mismo contexto siguió siendo una limitación documentada.

Pruebas registradas cubrieron avance/completion multibucket totalmente podado,
resume con mismo filtro tras append, rechazo de offsets incompletos, inicialización
de buckets vacíos, idle streaming y cancelación terminal. Polling nativo limitado
redujo trabajo pendiente decodificado a un lote por pull y retiró `VecDeque`
streaming. No quedó await de red tras decode en batch poll. Esperas de inicialización,
topología y polling streaming quedaron finitas sin convertir idle en deadline de
fin. Errores tipados fuente se separaron en `error.rs`; errores de protocolo
originales no cambiaron.

La evidencia asociada incluyó límites actuales, carga activa de 100 filas productor/
consumidor lento, nueve casos live y tres perfiles remotos repetidos, más timeout
inicial de failover. No se afirmó protección pre-descompresión, techo RSS total ni
aceptación benchmark.

## Escritura Arrow nativa

La ruta cliente Arrow pasó a usarse para INSERT log finito y continuo. Se retiraron
inferencia de partición por primera fila, agrupación por conteo de tabla y helper
`take` incondicional. Assigner `WriterClient` y un snapshot de metadatos inmutable
comparten agrupación/enqueue con conteos efectivos antiguos/nuevos. Grupos contiguos
usan slices; intercalados usan Arrow take en orden de entrada; slicing por bytes
objetivo conserva presión del limiter cliente existente.

El sink reutiliza leases propietarios de buffers para entrada log retenida y salida
cast/gather materializada, también desde providers no-Fluss. Scratch routing usa
estimación cliente admitida en pool de sesión nativo. Upsert/delete KV conserva
codificación wire requerida por filas y vista tipada, no simula formato Arrow-log.
Codificación wire, admisión posterior a asignación, estimaciones conservadoras
nested/scratch y perfiles finales de recursos/fallos siguen siendo límites explícitos.
Ver [inventario de escritura Arrow](arrow-write-inventory.md).

## Límites de escritura nativa

Se añadieron alcance común finito de preparación destino, presupuesto de metadatos
entre lotes y timeouts tipados por fase; validación/preparación de claves comparte
presupuesto ACK del lote. Techo de entrada aplica a inputs finitos, continuos y
providers no-Fluss. Scratch MERGE usa claves seleccionadas en vez del payload
completo; scratch reutilizable del encoder de valores KV pertenece al writer.

Se preservó aislamiento por ejecución y se corrigieron carreras de cierre/adquisición
limiter, append tras cierre y ownership de sender en cierre cancelado. Guards encoder
siguen lotes/Bytes nativos; guards de RPC enmarcado sobreviven drenado acotado seguro
ante cancelación; metadatos routing/cola persisten cobrados durante idle. Admisión
llamador ocurre fuera de locks de colas nativas. No se añadió pool/asignador,
transporte ni coordinador de reintentos alternativo.

Casos Docker reales cubrieron saturación log/KV, stalls ACK/metadatos/bootstrap, idle,
cancelación concurrente, fallo fuente, invalidación tabla/esquema/partición y
políticas ACK; cuatro regresiones SQL seguían pasando. Ownership/estimaciones,
recuperación observada, hang/limpieza del probe de prueba y límite frame de 30 s
están en [verificación de presión de escritura](write-pressure-verification.md).

## Retiro de FFI/Python y límite de aceptación

Se retiraron del workspace activo el bridge del proyecto, crates de adaptador
genérico de extensión/recursos/planes opacos, patch/build script host, adapters/
ejemplos/pruebas Python y configuración de paquete/lock/versionado. Workspace raíz
conservó solo `fluss-datafusion`; workspace de validación del cliente importado
conservó crates Rust y excluyó bindings de otros lenguajes. Fuentes importadas e
historial siguen como proveniencia, no integraciones activas ni entrega prometida.

Estas retiradas no bloquean limpieza/entrega Rust. Nuevos bindings requieren decisión
explícita tras aceptación completa de DataFusion nativo, incluida política runtime/
planificación del llamador, ciclo de vida y recovery. No se inventó implementación
de motor en esta limpieza. El backport del planner nativo genérico sigue siendo
necesario para selección DELETE y no depende de adaptadores de lenguajes externos.
