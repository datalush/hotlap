# Evidencia histórica de lecturas remotas

Resultados de laboratorio sobre retención remota, RustFS, S3/STS y fallos de
transporte. Límites y alcance del despliegue en
[preparación para producción](production-readiness.md).

## Casos registrados en un perfil independiente de servidor Docker

Las pruebas `remote_retention` ignoradas usan imagen Fluss 1.0, filesystem local
compartido con tabletserver o endpoint S3 RustFS existente, segmentos log de 120
bytes y comprobaciones tiering/retención de un segundo. La prueba S3 aísla objetos
bajo prefijo único en `fluss-lab` y lo elimina al terminar. Ambas verifican el
**contador real de bytes descargados remotamente** mientras DataFusion devuelve
exactamente las filas proyectadas/filtradas. Con consumidor remoto detenido se
comprueban límite de precarga de cuatro archivos, reserva DataFusion estable y
limpieza de temporales tras cancelar.

Al cambiar `table.log.ttl` de desactivado a dos segundos durante lectura, scanner
limitado a un segmento remoto precargado y una fila por pull no cachea segmentos
no leídos. Tras avanzar retención, scan abierto falla con error de rango o segmento
ausente, en vez de devolver resultado incompleto; consulta nueva devuelve exactamente
filas aún retenidas. Scan log continuo pausado en primera fila remota también falla
explícitamente tras avanzar retención, sin saltar registros perdidos. Se verificó en
perfil RustFS con imagen publicada `.6` y política STS de solo lectura restringida
al prefijo. Presupuesto de una solicitud pendiente y presupuesto de precarga remota
de un byte hacen fallar scan grande, no truncarlo. Slots de solicitud remota se
liberan al cancelar.

Credenciales temporales S3 recibidas como `security_token` o `session_token` se
entregan a OpenDAL como propiedad S3 `session_token`. Si ambos nombres tienen valores
distintos, cliente los rechaza sin registrar tokens. Gestor publica expiración del
token Fluss con cada actualización. Si falla refresh y expira token anterior, nuevas
descargas esperan hasta `scanner_remote_log_operation_timeout_ms` un reemplazo válido;
el gestor también limita su RPC de token con ese plazo y shutdown interrumpe fetch
bloqueado. `Debug` de credenciales oculta claves y tokens. Readers no envían a
OpenDAL el token vencido. Timeout global DataFusion sigue acotando lectura completa,
incluidas esperas de credenciales/reintentos. Pruebas unitarias inyectan fallos
OpenDAL temporales, comprueban recuperación dentro del presupuesto y siguiente
lectura exitosa; prueban aparte presupuesto agotado y refresh bloqueado.

Un perfil de fallos ignorado enruta HTTP S3 por proxy de prueba efímero hacia RustFS
**existente**. Inyecta dos respuestas 503 y recuperación, 503 persistente hasta agotar
reintentos y respuesta retenida para timeout/cancelación; luego una consulta DataFusion
nueva tiene éxito. Llamadas STS van directamente a RustFS y solo reciben fallos las
lecturas firmadas STS del prefijo de prueba. El perfil también hizo fallar explícitamente
un scan log **no acotado** tras agotar tres intentos HTTP 503 reales, sin lote parcial.
La ejecución duró ~36 s con 32 filas e imagen publicada `.6`.

Perfil independiente de expiración real ignorado pausa scan log activo tras su primera
fila remota firmada STS. AssumeRole predeterminado Fluss dura una hora; endpoint STS
solo de prueba solicita sesiones reales de **900 segundos** al mismo RustFS, conservando
política inline de solo lectura. Token inicial emitido por servidor enumeró prefijo
antes de expirar y RustFS vivo lo rechazó después (`InvalidRequest`); Fluss obtuvo
segunda sesión y el *mismo scan pausado* devolvió las 32 filas. Proxy S3 observó
fingerprints de session-token distintos antes/después de expiración. Terminó en ~938 s
con imagen publicada `.6`.

El laboratorio RustFS también tiene usuario IAM **`fluss-read`** y política limitados
al bucket: credenciales directas y asumidas leen, pero no escriben (403). No es el
uploader del servidor Fluss. RustFS acepta `RoleArn` por compatibilidad, pero sesiones
firmadas con claves raíz del servidor heredan permisos root salvo que `AssumeRole`
incluya `Policy` inline. La imagen publicada
`ghcr.io/midnattsol/fluss:1.0.0-midnattsol.6` contiene fix opcional
`s3.assumed.role.policy` (revisión `ff40eadf0`). Con política restringida al prefijo
de prueba aislado, subió segmentos log y emitió tokens capaces de leer objeto RustFS
real, pero sin permiso PutObject/DeleteObject; scan DataFusion S3/TTL pasó. La misma
imagen `.6` pasó también sin política, conservando comportamiento predeterminado.
Imagen `.5` aún emite sesiones derivadas de root sin restricción; `.6` requiere
configurar política explícitamente para afirmar mínimo privilegio en producción.

Downloader informa segmento ausente como scan incompleto (objeto expirado/eliminado);
conserva error storage como causa sin asumir TTL. Errores permanentes de storage no
se reintentan.
