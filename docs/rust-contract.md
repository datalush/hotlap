# Contrato Rust de Fluss y DataFusion

Registro de decisiones del 2026-10-04. Este es el **contrato objetivo** canónico
para Rust nativo; no declara que todos sus requisitos estén implementados.
[La auditoría](rust-implementation-audit.md) registra evidencia del código y
[la semántica de lectura](reading-semantics.md) describe la implementación actual.
Se retiraron FFI/Python del proyecto activo. Reconsiderar bindings requiere
aceptación completa de Rust nativo y del motor consumidor, además de una nueva
decisión explícita de alcance; no es una fase de entrega programada.

## 1. Decisiones de API y responsabilidades

Se conservan `FlussLogTable`, `FlussKvTable`, `FlussCatalog`, `LogReadOptions`,
`LogReadMode`, `LogStart`, `LogDelivery` y `FlussWriteOptions` como los
puntos de entrada. SQL usa `TableProvider`, `SessionContext`, streams y
`DataSinkExec` de DataFusion. No se introduce otra sesión, ejecutor SQL,
planificador, framework de operadores, gestor de memoria ni controlador de reintentos.

- El cliente Rust es responsable del protocolo/autenticación, metadatos,
  enrutamiento, decodificación/codificación, colas, solicitudes, reintentos y ACK.
  El enrutamiento correcto de lotes Arrow pertenece al cliente.
- El conector Rust adapta los contratos de DataFusion a operaciones del cliente,
  valida la identidad/esquema del destino, declara límites y expone observaciones fieles.
- DataFusion es responsable de la evaluación SQL exacta, planificación lógica y
  física, distribución, ejecución de operadores y su pool de memoria. La composición
  Rust nativa debe respetar la planificación del llamador, no compensar FFI.
- El motor gestiona checkpoints procesados, recuperación de trabajos, políticas
  de conflicto y conciliación tras escrituras inciertas. El progreso de fuentes y
  los ACK son datos para esas decisiones, no sustitutos.

### Matriz de capacidades

| Operación / propiedad | Provider de log | Provider KV |
| --- | --- | --- |
| Lectura finita | Rango de offsets retenidos capturado por bucket | Filas vigentes mediante sesiones de snapshot paginadas por bucket |
| Lectura continua | Modo streaming explícito | No soportada: este contrato no incluye changelog KV |
| Posición inicial | Más antiguo retenido / último capturado / offsets explícitos completos por bucket | Snapshot actual por bucket, sin offsets de log |
| INSERT | Append | Upsert de fila completa |
| Entrada continua a INSERT | Soportada; confirma lotes sin esperar EOF | Upsert completo soportado, misma regla de confirmación |
| DELETE | No soportado | Selección exacta sobre snapshot finito y luego borrado por clave si la política lo permite |
| MERGE | No soportado | Fuente finita; cláusulas soportadas ordenadas y upsert completo/borrado por clave |
| INSERT OVERWRITE / REPLACE | No soportado | No soportado |
| UPDATE / TRUNCATE directos | Fuera de alcance | Fuera de alcance; incluye cláusulas UPDATE dentro de MERGE |
| Snapshot global / atomicidad de sentencia / CAS condicional | No garantizados | No garantizados |
| Exactly-once tras reinicio o pérdida de ACK | No garantizado | No garantizado; repetir un upsert completo no hace exactly-once al trabajo entero |

Las capacidades describen el **soporte del conector y los metadatos observados**,
no autorizan la ejecución ni garantizan que los metadatos permanezcan iguales.
Una política que prohíbe o ignora borrados hace que DELETE no esté disponible;
la autorización y la identidad del destino se verifican igualmente al ejecutar.
Consultar capacidades no debe escribir datos ni abrir silenciosamente otra tabla.

`FlussLogTable::capabilities()` y `FlussKvTable::capabilities()` exponen
`FlussCapabilities`: `FlussReadCapability` y `FlussInsertCapability` elegidas,
soporte de `delete` y MERGE restringido. El indicador de borrado KV refleja la
política observada al abrir el provider. Un provider nuevo vuelve a capturar los
metadatos observados por el cliente, que no necesariamente son los más recientes
del servidor; la ejecución valida metadatos y política. MERGE no anula un indicador
de borrado falso para sus cláusulas DELETE. Esta vista inmutable no concede permisos
ni es un registro mutable o una extensión del planificador.

### Uso existente (no son APIs nuevas)

```rust
use std::{sync::Arc, time::Duration};
use datafusion::prelude::SessionContext;
use fluss::metadata::TablePath;
use fluss_datafusion::{FlussLogTable, LogReadOptions, FlussWriteOptions};

// Dentro de una función async, con un Arc<FlussConnection> existente llamado connection:
let events = FlussLogTable::open_with_options(
    Arc::clone(&connection),
    TablePath::new("db", "events"),
    LogReadOptions::batch(Duration::from_secs(45)),
).await?.with_write_options(FlussWriteOptions::default())?;
let ctx = SessionContext::new();
ctx.register_table("events", Arc::new(events))?;
let rows = ctx.sql("SELECT COUNT(*) FROM events").await?.collect().await?;
```

`FlussLogTable::open` sigue siendo el punto de entrada cómodo para lectura finita.
`open_with_options(LogReadOptions::default())` elige streaming explícitamente;
consuma el stream de DataFusion o use un plan que termine, no un `collect()` finito
incondicional. KV se registra con `FlussKvTable::open` y el mismo `register_table`
nativo. El modo/posición inicial del provider y las opciones de escritura son
independientes de la configuración de transporte del cliente y de SQL/memoria de DataFusion.

## 2. Corrección de lectura, orden y progreso

### Selección y finalización

- La lectura batch de log resuelve inicios y finales por bucket al ejecutar, no al
  registrar. El inicio es inclusivo y el final exclusivo. Las capturas no son un
  snapshot transaccional simultáneo entre buckets.
- Los snapshots KV se abren perezosamente por bucket y sus páginas continúan la
  misma sesión. Una sesión fallida nunca se reinicia en silencio sobre un snapshot
  más reciente. No se garantiza un snapshot KV global durante escrituras concurrentes.
- Cada bucket seleccionado debe asignarse exactamente una vez entre particiones,
  según su layout efectivo, incluidas particiones antiguas tras un rescale.
- Los límites SQL son operadores globales de DataFusion. La poda de lotes o
  particiones no reemplaza los filtros exactos por fila; se conserva la evaluación residual.
- Una lectura puede ofrecer lotes y luego fallar. Eso no equivale a una consulta
  completa y exitosa: no se trunca en silencio por timeout, pérdida de retención
  o invalidación de metadatos. El consumidor conserva el estado terminal.
- Los offsets de bucket son la referencia de orden en logs. No se promete orden
  entre buckets/particiones ni en SQL sin orden explícito. KV no declara orden
  global; quien lo requiera usa los operadores pertinentes de DataFusion.
- Cambios de metadatos pueden requerir replanificar/reiniciar con posiciones válidas;
  los cambios de topología no soportados durante ejecución fallan explícitamente.
  No se exige descubrimiento ni recuperación automáticos. La identidad incluye el
  ID de tabla, no solo ruta/nombre.

### Progreso: ofrecido no significa procesado

El contrato de observación de fuentes amplía el propósito de `LogDelivery`; no
debe reinterpretar en silencio sus eventos de lote existentes.

Observaciones requeridas:

1. **Inicialización:** identidad de ejecución y tabla/esquema, layout de
   particiones/buckets seleccionados e inicios/finales resueltos (si es finita),
   incluidos rangos vacíos. La identidad existe aunque no se ofrezcan lotes.
2. **Lote ofrecido:** bucket, base inclusiva, siguiente posición exclusiva y filas.
3. **Rango excluido / avance:** avance por rangos excluidos concluyentemente por
   el predicado de fuente, diferenciado de datos ofrecidos.
4. **Estado terminal:** finalización, fallo o cancelación, con integridad del
   stream de observación conocida por su consumidor.

Reglas:

- Una posición reanudable no puede sobrepasar datos del bucket aún almacenados
  en buffer y no ofrecidos. Fetch/consumo del scanner pueden adelantarse a la
  entrega; son diagnósticos, no posiciones de reanudación intercambiables.
- La exclusión solo vale con la misma semántica de selección. Otro filtro, inicio
  o layout no permite asumir que los datos omitidos se procesaron.
- SQL aguas abajo puede descartar un lote ofrecido; la observación de fuente no
  sabe si el motor lo procesó o confirmó su sink/estado.
- Desbordamiento o rezago del observador significa evidencia incompleta. No se
  omiten eventos en silencio para luego llamar al registro completo/reanudable.
- La observación es opcional y acotada; la pérdida debe detectarse sin hacer que
  SQL ordinario dependa de un consumidor de checkpoints del motor.
- Ejecuciones concurrentes independientes usan `TaskContext` nativos distintos.
  La identidad actual no soporta solapamiento del mismo plan con el mismo contexto;
  ese límite se documenta y prueba, sin inferir identidad de la reutilización de
  punteros. Una reejecución secuencial vuelve a capturar.

`LogDelivery` conserva el significado de solo-lotes-ofrecidos. `subscribe_progress()`
devuelve el receiver broadcast acotado de Tokio para `LogProgress`: inicialización
(ejecución/tabla/esquema, partición/conteo y rangos `LogReadPosition`), Offered,
Excluded y Terminated (`LogTermination`). Trate `RecvError::Lagged` y
`TryRecvError::Lagged` como evidencia incompleta: la fuente no puede certificar
un consumidor que descartó eventos. No derive un mapa reanudable completo sin
inicializaciones de todas las particiones. La validación de offsets explícitos
rechaza mapeos de bucket ausentes, obsoletos o fuera de rango; la compatibilidad
de consulta/selección y los checkpoints procesados corresponden al motor. No es
un servicio de checkpoints.

## 3. Recursos y plazos

El detalle de propiedad de memoria, presupuestos, límites de cancelación y
reintentos está en [recursos del contrato Rust](rust-contract-resources.md).

## 4. Escrituras y resultados

Conteos, ACK, observabilidad, DELETE y MERGE se detallan en
[escrituras del contrato Rust](rust-contract-writes.md).

## 5. Errores y límites de aceptación

La política de clasificación de errores y la evidencia histórica de escenarios,
pruebas y límites se conserva en [evidencia del contrato Rust](rust-contract-evidence.md).
Se mantienen las causas originales, sin inferir seguridad de reintento de un
mensaje. Los contratos públicos actuales aparecen arriba.
