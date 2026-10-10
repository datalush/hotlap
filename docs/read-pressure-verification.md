# Verificación nativa de presión y cancelación de lectura

Evidencia de árbol de trabajo 2026-10-04, posterior a `f63a4f5` / `6f0ef1d`.
Endurecimiento funcional, no aceptación final de rendimiento sostenido.

## Cambios y límites efectivos

| Recurso/espera | Comportamiento aplicado | Límite de la afirmación |
| --- | --- | --- |
| Lotes Arrow decodificados por poll fuente | Streaming DataFusion y reader log acotado usan `poll_with_batch_limit(..., 1)` nativo | Lote aún puede ser grande; decode precede admisión pool |
| Cola streaming decodificada | Se retiró VecDeque conector; poll devuelve cero/un lote; exceso es error explícito | Fetch raw/colas descarga remota son recursos cliente aparte |
| Poll bulk cliente | `poll()` conserva máximo 100 lotes; API limitada valida 1..=100 | Cap bulk 64 MiB raw/decodificado sigue siendo blando |
| Buffers entregados | Leases pool sobreviven retención consumidor, clones/slices y cancelación stream | Cargo por lote conservador; no cubre toda salida SQL ni RSS |
| Cancelación tras decode | Poll batch devuelve datos decodificados sin esperar otro fetch/metadatos | Fetch nuevo inicia al drenar buffer existente; efecto throughput requiere perfil |
| Esquema ausente | Collector devuelve lotes decodificados antes de esperar esquema y conserva cursor raw al cancelar | Cachés/transporte metadatos siguen mecanismos cliente |
| Ejecución fuente | Deadline batch/KV compartido sin cambios | No es deadline de consulta SQL completa |
| Inicialización/topología streaming | Timeout finito usa `scanner_remote_log_operation_timeout_ms` de conexión | Reutiliza allowance existente, no segunda pila reintentos |
| Poll streaming | Mismo allowance más intervalo normal idle | Idle no es timeout final; fuente puede permanecer idle entre polls |
| Disco/prefetch | Se conservan permisos remotos de slots y bytes realmente escritos | Recurso distinto de RAM Arrow; prefijos S3 aislados/solo lectura siguen vigentes |

No hay espera de red mientras batch poll conserva salida decodificada lista para
consumidor. Antes, `send_fetches().await` ocurría después de recopilar lotes; error/
cancelación allí podía descartar datos con offsets consumidos avanzados. Fetch inicia
ahora con poll vacío/drenado, no tras pausar consumidor con salida nueva. Salida
reader acotado sigue siendo un lote por vez; no se añade scanner ni transporte.

Errores locales de fuente están en `error.rs`, separados de propiedad buffers:
`FlussScanTimeout`, `FlussOperationTimeout`, `FlussReadInvalidated` y sus causas
Identity/Schema/Topology/Retention se alojan en errores External DataFusion nativos.
Errores Fluss/RPC/storage conservan cadena original, sin wrapper en jerarquía duplicada.
Mensajes Display siguen compatibles; invalidación nunca cambia tabla/snapshot/offsets
silenciosamente.

## Evidencia funcional registrada (DEBUG, ocho jobs)

- **21 pruebas unitarias core**, incluidas retención/expiración fuente tipada, leases,
  generaciones deadline, offsets completos y fronteras progreso seguras.
- **14 pruebas de fetch cliente**, incluido decode de lote único que conserva otro
  bucket completo para poll posterior, límites fetch configurados, colas podadas,
  error retención y error tras decodificar otro bucket.
- **Las nueve integraciones native-sni de lectura**: log/KV batch, snapshot/evolución,
  vacío/reejecución/concurrencia, descubrimiento particiones/rescale, tabla recreada/
  esquema cambiado, rechazo TLS, cambios topología streaming, idle/cancelación y
  failover coordinador con recuperación consulta nueva.
- Fixture pressure streaming produce **100 filas únicas en 20 grupos confirmados**,
  consumidor pausa 70 ms/lote mientras productor sigue append. Pool fuente 1 MiB.
  Reservas estables durante pausa; vuelven a cero al soltar último owner lote; drop
  stream deja cero streams activos. Verifica cada ID esperado sin duplicados.
- **Cuatro integraciones SQL escritura** pasan con cadencia fuente input cambiada:
  planner nativo, input multipartición, contrapresión, INSERT continuo y layouts
  partición antiguo/nuevo.
- **Tres integraciones Docker/RustFS/remotas** pasaron en imagen existente `.6`:
  retención filesystem/limpieza remota, perfil S3 retención/solo lectura y fallos HTTP
  S3 transitorios/permanentes con cancelación/limpieza de objetos aislados. Aserciones
  consumidor remoto lento retienen lote explícitamente: drop stream no elimina lease
  de datos aún retenidos por consumidor.
- Clippy core all-targets/all-features con warnings denegados y formato pasaron.

Primera corrida native-sni completa tuvo timeout `Elapsed` sin etiqueta de fase en
failover, mientras otras ocho pruebas pasaron. Pod había sido reemplazado y estaba
Ready. Se añadieron diagnósticos de timeout con contexto sin ampliar/suprimir límites;
rerun dirigido y dos corridas completas posteriores de nueve pruebas pasaron. La fase
precisa del timeout inicial no quedó registrada; se conserva como observación
ambiental/funcional, sin afirmar éxito de esa corrida ni causa raíz probada.

## Evidencia reutilizada y seguimiento

Controles retención conector añadidos tras aclarar alcance: log/KV
`with_max_retained_batch_bytes` por defecto 64 MiB, rechaza cero, comprueba suma de
capacidades respaldo con overflow protegido y falla antes de adquirir lease si lote
excede límite. No parsea IPC ni reemplaza política `MemoryPool` nativa. EXPLAIN
informa límite; gauge nativo retained-source-bytes se equilibra con vida leases.
Pruebas cubren admisión exacta, views slice que retienen respaldo mayor, rechazo sin
afectar leases activos ajenos y rechazo real log/KV con pool host no acotado. Conteo
unitario actualizado: 22; pasan log/KV acotados, presión/cancelación continua y las
cuatro integraciones escritura.

Motor/aplicación configura política/concurrencia. Ownership/admisión es responsabilidad
conector; límites asignación-time formato/descompresión pertenecen cliente/Arrow. No
se añade estimador IPC en conector ni decoder duplicado.

- Perfil sostenido Rust de 5+30 min sigue siendo baseline histórico, no benchmark
  nuevo de cadencia pull.
- Evidencia real expiración/renovación STS 900 s se reutiliza: auth/token no cambió y
  corrida larga renovación no se repitió. Casos cortos reales HTTP/retención/limpieza
  descarga se repitieron por cambio polling.
- Contratos modo/idle/topología continúan; casos live actuales se repitieron. Progreso
  representa trabajo source ofrecido/excluido, no checkpoints procesados.

Perfiles nativos miden latencia/throughput/RAM/disco sostenidos bajo pipeline final.
Verificación fallos cubre matriz integrada fallos/permisos. Decode lote único grande,
respuestas raw, expansión compresión y asignaciones metadatos cliente siguen fuera de
protección pool preasignación; límite conteo lotes o perfil representativo estable no
son techo RSS universal. No incluye build Python/FFI, mmap, gestor global asignaciones
ni política reintentos nueva.
