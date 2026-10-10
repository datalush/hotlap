# Memoria, recursos, plazos y cancelación

Extracto técnico del [contrato Rust](rust-contract.md).

## 3. Memoria y propiedad de recursos

`MemoryPool` contabiliza/admite consumidores registrados; no es un asignador que
cubra automáticamente el RSS del proceso. No describa `pool_limit` como límite
total de memoria ni suponga que `RecordBatch::clone` duplica el payload.

Ambos providers exponen `with_max_retained_batch_bytes(bytes)` (positivo; por
defecto 64 MiB). Rechaza lotes cuyas capacidades Arrow de respaldo excedan ese
techo, independientemente del pool disponible. El pool nativo sigue siendo la
política compartida; no se añade otro pool/contador asignador. Es **admisión de
retención posterior a decodificar**, no protección contra asignaciones o expansión
del decoder. El límite aparece en EXPLAIN y el gauge nativo
`fluss_retained_source_buffer_bytes` sigue leases de buffers, incluso si las
salidas se retienen tras completar/eliminar el stream. Es conservador con respaldos
compartidos/proyectados y no equivale al RSS ni a todas las reservas del pool.

El motor/aplicación elige presupuestos/concurrencia y `MemoryPool`. El conector
admite/retiene contra ese pool y propaga ajustes del cliente nativo; cliente/Arrow
se encargan del parseo de formato y límites durante la asignación. La inspección
previa a decodificar y el endurecimiento de codecs pertenecen allí, no a un parser
IPC del conector, asignador global, planificador ni bucle de recuperación.

| Recurso | Responsable y contrato requerido | Estado actual / límite |
| --- | --- | --- |
| Lotes fuente pendientes decodificados | Cliente los produce; el conector cobra colas tras polling y antes de emitir | Contabilización implementada; transitorios previos a decodificar/poll incompleto fuera de la reserva del lote emitido |
| Lotes fuente ofrecidos | Propietarios Arrow retienen leases del respaldo mediante clones/slices/proyecciones hasta liberar el último buffer | Implementado; lotes retenidos siguen cobrados tras completar/cancelar el stream |
| Estado retenido por operadores | Operadores nativos DF usan su pool real de sesión cuando reservan recursos | Se conserva contexto y planificación nativos |
| Lotes Arrow de entrada/gather del sink | Leases de respaldo log siguen buffers retenidos por cliente; guard KV cubre codificación por filas; scratch de routing usa el mismo pool | Propiedad columnar implementada; evidencia de saturación/bytes depende del perfil |
| Cola codificada, frames RPC, routing persistente | Límite del cliente y guards del pool DF para batches/Bytes/frames/cache | Implementado; estimaciones y picos requieren perfiles sostenidos |
| Conjunto de claves MERGE y scratch | Representación/overhead PK en pool DF; encoder de valores por filas tiene reserva separada | Scratch de claves implementado; cobertura semántica se verifica aparte |
| Archivos remotos/slots de descarga | Permisos existentes del cliente para bytes en disco/concurrencia, bytes reales y limpieza | Presupuesto aparte del cliente |
| Respuestas crudas/descompresión/asignaciones temporales | Límites y comportamiento transitorio explícitos del cliente | No cubiertos por la reserva del lote emitido |

Invariantes requeridos:

- Los presupuestos de cola y ejecución son finitos/observables en perfiles
  aceptados; el exceso se rechaza claramente, no se trunca en silencio.
- El tamaño decodificado desconocido puede requerir asignación transitoria.
  Declárese y mídase ese límite; una reserva posterior no protege asignaciones previas.
- Compartir arrays entre source/sink puede causar cargos conservadores superpuestos.
  No se equiparan totales contables a bytes únicos asignados ni se libera un guard
  real solo para reducir la cifra reportada.
- Los presupuestos se agregan entre ejecuciones concurrentes: límites por writer
  se multiplican. No se implementa otro coordinador global dentro del conector.
- Drop/error/cancel libera colas/permisos propios en tiempo acotado; arrays Arrow
  retenidos externamente siguen válidos y pertenecen a sus consumidores.
- Bytes de datos, picos por lote, colas, reservas del pool y RSS son métricas
  distintas. Un gauge de pico por lote no demuestra límites de colas pendientes.

Las vistas/slices/proyecciones Arrow preservan almacenamiento cuando la semántica
lo permite. Decodificación por filas, normalización de tipos, gathers, resultados
SQL, compresión y codificación de wire pueden asignar memoria. Afirmar cero copias
requiere evidencia de identidad/rango de buffers y ciclo de vida en el límite
nombrado, no solo igualdad de valores o el nombre de una API Arrow.

## 4. Plazos, reintentos y cancelación

| Ámbito | Decisión contractual | Estado de implementación |
| --- | --- | --- |
| Ejecución de fuente batch | Presupuesto común desde la primera partición física; incluye espera de descubrimiento/captura/apertura/poll/decodificación. No es un plazo para toda la consulta SQL | Plazo compartido implementado; los demás límites dependen del cliente |
| Ejecución de fuente KV | Igual principio, incluidas esperas de página; snapshot inválido falla en vez de reiniciarse | Plazo compartido implementado; snapshots inválidos fallan |
| Fuente streaming | Sin timeout de finalización para entrada normalmente idle; operaciones de red/almacenamiento/metadatos tienen esperas finitas aplicables | Se conserva idle; límites operativos dependen del cliente |
| Preparación/metadatos del destino de escritura | Alcance común finito para conexión/tabla/particiones y comprobación acotada entre lotes | `preparation_timeout` implementado (30 s por defecto); el catálogo de lectura tiene contrato separado |
| Enqueue + ACK de un lote | Un plazo comienza antes de validar/admitir/codificar claves; reintentos/backpressure nativos comparten el plazo | Implementado con causa tipada EnqueueAndAck y cobertura de saturación log/KV |
| Espera del siguiente lote streaming | Espera normal de entrada, no vencimiento de un ACK de lote pendiente | Sin lote no enviado no hay reloj ACK que venza |
| Limpieza del writer | Límite graceful al completar; abort/cierre nativo cooperativo al drop; frames RPC parciales tienen drenado finito independiente | Corregidas carreras AbortHandle/limiter; recuperación de frame pequeño observada ≤3 s y frame detenido acotado a 30 s; ver verificación de fallos |

Se conservan los nombres/valores predeterminados existentes de `FlussWriteOptions`:
`ack_timeout = 30s`, `max_retries = 3`. `ack_timeout` debe poder representarse como
al menos 1 ms porque las esperas de buffer del cliente usan milisegundos; máximo
3600 s. Se rechazan duraciones positivas inferiores a un milisegundo para que la
conversión no produzca timeout cero en silencio. El presupuesto de reintentos es
positivo (cero no se soporta actualmente) y limita el mecanismo del cliente; no es
un nuevo bucle del conector ni promete un número fijo de envíos físicos sin verificar
el uso del cliente.

Las opciones Rust de escritura también exponen `preparation_timeout=30s`
(1 ms..=3600 s) y `max_retained_batch_bytes=64 MiB` positivo. El techo de escritura
se aplica después de materializar a todos los providers de entrada, no solo a
salidas de fuentes Fluss. Codificación, transporte enmarcado, caché de routing y
scratch KV reutilizable usan el mismo pool nativo de sesión; las reservas siguen
a sus propietarios, incluidos writers idle y drenados de frames cancelados. Ver
[verificación de presión de escritura](write-pressure-verification.md) para
estimaciones de admisión, límites de limpieza RPC/kernel y fallos reales log/KV.

No se renombra `batch_timeout` ni se describe como plazo SQL global. Los comentarios
públicos expresan el plazo compartido de ejecución de fuente, no de toda la consulta.
Las opciones de preparación deben ser explícitas y pequeñas; no se añade gestor
general de plazos ni herencia de reintentos sin límite.

La cancelación deja de aceptar entradas, señaliza el enqueue bloqueante, aborta el
writer dedicado y despierta esperas de buffers. Cancelar un `JoinHandle` async no
detiene por sí solo una closure bloqueante en ejecución. La reserva del lote de
entrada persiste hasta que esa closure lo libera. Ningún ACK, timeout, abort o
cierre de conexión deshace retroactivamente solicitudes ya aplicadas por Fluss.
