# Evidencia histórica del contrato Rust

Registro de escenarios, decisiones y límites que sustentan el
[contrato Rust](rust-contract.md). Los fragmentos de código conservan los
identificadores originales de API.

## 7. Escenarios de aceptación y responsabilidades

| Escenario | Evidencia requerida |
| --- | --- |
| Registro log/KV, DML no soportado y política de borrado | Capacidades veraces; rechazo antes de enviar; permisos verificados aparte |
| Planificador/optimizador nativo personalizado, entrada multipartición | Composición sin bypass motivado por FFI; consumo de todas las filas previstas |
| Retener varios lotes fuente y solicitar más | Buffers válidos, límites explícitos de contabilidad/colas; separar cargo de fuente y bytes retenidos |
| KV ancho proyectado y escritura de lote log compatible | Esquema/NULL correctos; materializaciones y buffers medidos |
| Particiones mixtas con conteos antiguos/nuevos | Routing correcto por fila; agrupación por layout efectivo |
| Fuente vacía/toda podada y particiones tardías | Identidad/asignación inicial completa y progreso sin saltar cola |
| Reejecución/concurrencia | Capturas nuevas; contextos distintos aislados; solapamiento no soportado documentado |
| Partición tardía, espera de metadatos, stream idle | Presupuesto común donde corresponde; idle no vence finalización streaming |
| Buffer saturado, espera ACK y cancelación | Enqueue despierta; workers liberan lotes/permisos a tiempo; no se afirma rollback |
| ACK perdido/error de entrada/limpieza tras ACK | Conteos conocidos preservados; resultados inciertos no inventados; SQL falla |
| Cambios concurrentes de filas DELETE | Conteo de operaciones ACKed, no borrado condicional/neto |
| Duplicado MERGE en lote tardío, payload no-clave ancho | Efectos anteriores retenidos, duplicado detectado, presupuesto de clave/scratch adecuado |
| Writer_acks=1 vs all y replay | Garantías distintas declaradas; append puede duplicar; no inferir exactly-once |

Las pruebas funcionales usan DEBUG/ocho jobs; perfiles/benchmarks/entrega usan
RELEASE/ocho jobs. Se reutiliza evidencia histórica cuando el camino no cambió y
se repiten las pruebas afectadas. La aceptación final requiere fallos/perfiles y
reproducción desde checkout limpio. También identifica y valida el motor DataFusion
nativo de este repositorio: planificación/runtime del llamador, concurrencia,
ciclo de vida source/sink, cancelación/reejecución y recovery. Las pruebas del
provider por sí solas no aceptan ese motor y bindings no las sustituyen.

Fuera de la implementación del conector quedan trabajos/checkpoints persistentes,
recovery global, políticas de conflicto de negocio y transacciones multibucket;
cuando se necesiten, corresponden a la aplicación/motor, no a un nuevo gate de
planificación/checkpoints para aceptar DataFusion nativo. Changelog KV/snapshot-
changelog y UPDATE/TRUNCATE directos son extensiones separadas. Las pruebas del
provider no prueban límites totales de RSS, orden global, rollback de sentencia ni
exactly-once del trabajo completo.
