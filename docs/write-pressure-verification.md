# Recursos de escritura y verificación de fallos nativos

Esto cubre ruta de ejecución Rust nativa, no aceptación Python/FFI ni benchmark
sostenido. Builds funcionales son DEBUG con `CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0`.

## Propiedad y admisión

Todo consumidor de escritura del conector se registra en pool DataFusion real del
`TaskContext` suministrado. Cliente conserva propiedad de limiter buffers, protocolo,
routing, colas, ACK y reintentos. `WriterMemoryAccounting` transfiere guards de
admisión a owners nativos; no es otro pool/asignador ni política reintentos.

| Recurso | Admisión / propietario | Liberación |
| --- | --- | --- |
| Entrada log, cast y buffers gather | Leases respaldo Arrow existentes; límite entrada/gather | Al liberar buffer correspondiente, incluso slice retenido por cliente tras drop worker |
| Lote de entrada KV | Reserva worker | Al terminar/drop worker; cancelar llamador no libera lote retenido |
| Scratch routing/grupos | Estimación cliente admitida en worker | Al terminar worker |
| Metadatos persistentes assigner/cola | Guards nativos `reserve_routing`, `FlussWriteRoutingMetadata` | Sustituir assigner o destruir writer/accumulator privado |
| Lotes codificados y scratch codec | Admisión nativa antes de construir lote, `FlussWriteEncoded` | Al liberar lote y clones `Bytes` codificados |
| Bytes RPC enmarcados | Capacidad real Vec admitida tras serializar, `FlussWriteTransport` | Al terminar envío/drenado frame, incluso con llamador cancelado |
| Scratch reusable encoder fila/clave KV | Allowance conservadora de dos respaldos input más metadatos, propiedad de `FlussWriter` | Último owner del writer, incluido worker activo de query cancelada |
| Codificación claves duplicadas MERGE | Bytes respaldo de columnas PK seleccionadas, no payload no-clave arbitrario | Termina codificación temporal; conjunto claves retenido dura todo MERGE finito |

Estimaciones de codificación son **aproximaciones**, no heap exacto: Arrow
preconstruido usa cuatro veces hint sin comprimir/framing +4 KiB; KV usa dos veces
capacidad builder nativo +4 KiB; builders log nativos por fila usan cuatro veces
capacidad +4 KiB. Incluyen trabajo de builder/cache/compresión reutilizable; bytes
RPC enmarcados tienen guard propio. Routing persistente reserva 4 KiB por ruta
física/assigner y 512 bytes por bucket. Reservas siguen visibles con input idle,
no se supone que memoria writer desaparezca al ACK. Afinar perfil, RSS real y picos
asignador son mediciones separadas.

Admisión llamador ocurre fuera de locks deque nativos. Pool puede rechazar pronto,
no esperar a que consumidor libere memoria. Espera de buffers sigue en limiter
bloqueante del cliente. Rechazo local memoria RPC no se traduce a reintento de red:
causa tipada `WriterMemoryAdmission` y DataFusion original permanecen accesibles en
fallo flush.

Cast/gather Arrow y framing RPC asignan antes de admisión de retención. Pool es
contabilidad cooperativa, no asignador global ni techo RSS estricto. Leases de lote
completo source/sink pueden solaparse conservadoramente; slices clonados no implican
segunda asignación payload. Permisos cliente nativo son recurso configurado aparte;
límites por writer se multiplican entre consultas concurrentes.

## Opciones y plazos

`ack_timeout=30s` y `max_retries=3` conservan nombres/valores predeterminados.
`FlussWriteOptions` Rust expone además:

- `preparation_timeout=30s`, representable en **1ms..=3600s**: allowance común para
  configurar conexión/tabla/partición y comprobación metadatos acotada independiente
  entre lotes de entrada.
- `max_retained_batch_bytes=64 MiB`, positivo: límite respaldo posterior a
  materialización para todos los providers de entrada, independiente de capacidad pool.

Deadline enqueue/ACK de lote empieza antes de trabajo esquema/NULL/admisión/claves
MERGE y cubre enqueue worker, espera buffers cliente, reintentos existentes y flush.
Validación/cómputo no deben iniciar envío tras vencer deadline. Espera idle luego de
ACK no tiene deadline de finalización de lote.

`FlussWriteTimeout` expone `FlussWritePhase::{Preparation, Metadata, EnqueueAndAck}`
en causa External DataFusion. Errores pool/transporte/API conservan causas propias.
Timeout/drop no revierte requests aplicadas por servidor ni establece certeza por
fila. Resultados de confirmación estructurados se describen en contrato observación.

Cada lote verifica identidad tabla/esquema. Ejecución particionada refresca
metadatos y admite particiones nuevas con sus conteos efectivos; cambiar/recrear
partición física ya conocida invalida ejecución abierta. Nunca adivina layout
bucket nuevo ni reinicia trabajo en silencio.

## Cancelación y limpieza

- El limiter cambia su estado cerrado bajo el mismo mutex acquire/wait,
  lo comprueba tras lock/despertar y antes de adquirir, y rechaza valores
  plazos de espera no representables. Append de cola comprueba cierre antes de
  mutar/readquirir.
- `WriterClient` nativo conserva `AbortHandle` aparte del join handle. Cancelar cierre
  graceful no puede desprender un sender no abortable.
- Cierre de conexión mantiene writer visible hasta terminar; abort/cierre prohíbe
  crear writer de reemplazo en esa conexión. Drop último cliente aborta sender.
- `spawn_blocking` es cooperativo: abort despierta esperas buffer y detiene bucles
  routing/fila; no interrumpe forzosamente un kernel Arrow activo. Leases quedan
  cobrados hasta terminar owners. Límite de entrada acota tamaño lote admitido.
- Frame RPC enviado parcialmente conserva framing mediante future send existente
  seguro ante cancelación, con techo finito de drenado/escritura de **30 segundos**.
  Guard frame sobrevive a cancelación. Fallo envenena conexión aunque llamador original
  ya no exista; no puede enviarse otra request tras frame parcial.

Integraciones de frame pequeño recuperan reservas pool en 3 s. Es evidencia
observada, no promesa universal de terminar cada kernel/socket en 3 s; prueba de
reloj virtual con frame detenido cubre aparte límite de 30 s.

## Escenarios verificados

`tests/write_pressure.rs` usa StreamingTable/PartitionStream DataFusion y fixture
Docker plaintext propio. Pausa solo su coordinador/tablet. Probe pool delega política
a GreedyMemoryPool y sincroniza pausa tras admisión de metadatos, para que prueba ACK
no ejercite accidentalmente solo metadatos. Gauge nativo de buffers disponibles
demuestra agotamiento previo a cancelar.

La matriz se ejecuta para **append log y upsert KV**:

1. ACK singleton antes de EOF; idle superior al allowance ACK sigue activo.
2. Buffer cliente lleno 64 KiB/lotes 32 KiB con tablet pausado; cancelar productor,
   cerrar input y liberar guards propios input/codificación/frame.
3. Writer privado concurrente sobrevive cancelación peer y completa al reanudar;
   nueva ejecución explícita también escribe correctamente.
4. Singleton completamente encolado espera ACK bloqueado y devuelve timeout tipado,
   nunca conteo SQL final exitoso.
5. Metadatos y bootstrap/preparación bloqueados tienen ámbitos tipados finitos propios.
6. Techo entrada de un byte rechaza con pool disponible, sin persistir fila.
7. Error fuente tras ACK falla sentencia y fila anterior sigue visible.
8. Recrear tabla y ADD COLUMN live invalidan ejecución abierta; input posterior no
   entra en tabla/schema nuevos.
9. ACK=0 rechaza sin escribir; ACK=1 e idempotencia desactivada confirma operación.
   Suite SQL existente también ejercita default all/-1.
10. Partición nueva se admite durante INSERT abierto; partición conocida recreada
    rechaza input posterior en vez de escribir en identidad nueva.

Las cuatro regresiones `write_sql` native-sni siguen pasando INSERT/DELETE/MERGE,
concurrencia, routing old2/new3, presión entrada4MiB/buffer cliente2MiB, rechazo
pool pequeño y entrada continua. Pruebas core cubren rechazo pool compartido real
y vida de owners. Pruebas cliente cubren append tras cierre, cierre graceful
  cancelado, Bytes codificados que retienen guards y drenado/envenenamiento de frame detenido.

Durante desarrollo, probe pool bloqueante reveló admisión bajo lock deque; llamadas
de política pasaron fuera de sección crítica y consumidores de codificación/transporte
se separaron. Dos ejecuciones con timeout externo dejaron fixtures propios pausados;
se reanudaron/eliminaron explícitamente. Fixture final tiene diagnósticos de fase,
deadline interno 90 s y teardown ante panic/error. Espera readiness de tablet y
asignación líder tras recrear tabla.

Verificación registrada: **835 pruebas cliente aprobadas, 2 ignoradas; core 25
aprobadas; SQL native-sni 4 aprobadas; matriz Docker log/KV completa aprobada**.
Clippy core/cliente/test-cluster `-D warnings`, formato y `git diff --check` pasaron.
No quedan contenedores propios `df-write-pressure`.

Comandos desde raíz del repositorio:

```sh
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.6 CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p fluss-datafusion --locked --test write_pressure -- --ignored --test-threads=1
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 uv run --no-project --env-file ../lab/.env cargo test -p fluss-datafusion --locked --test write_sql -- --ignored --test-threads=1
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p fluss-datafusion --locked --lib
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR="$PWD/target" cargo test --manifest-path clients/rust/Cargo.toml -p fluss-rs --locked --lib
```

Matriz permisos/failover/conciliación, conocimiento estructurado de escritura,
aceptación continua conjunta, semántica concurrencia DELETE/MERGE y perfiles tienen
aceptaciones propias. Proyecto activo no integra FFI/Python; aceptación nativa y del
motor consumidor precede cualquier alcance nuevo de bindings.

Una extensión añade fixture propio separado de pérdida/reinicio socket a este target.
Pruebas presión/recuperación pasaron juntas, con comprobaciones datos tras reinicio y
limpieza pool. SQL SASL/ACL real de solo lectura/autorizado se prueba en
`tests/authorization.rs`. Ver [verificación de fallos nativos](native-failure-verification.md)
para resultados, fixes de preservación de causa y frontera ACK/checkpoint observada
en perfil servidor de réplica única.
