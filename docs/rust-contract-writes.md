# Escrituras y conocimiento parcial

Extracto técnico del [contrato Rust](rust-contract.md).

## 5. Escrituras, confirmaciones y conocimiento parcial

### Conteo SQL

- `count` es el número de **operaciones** seleccionadas/enviadas y confirmadas
  según la política ACK soportada, no el neto de filas insertadas/cambiadas/borradas.
- INSERT log agrega. INSERT KV hace upsert de fila completa; dos operaciones en
  la misma clave pueden contarse dos veces y dejar una sola fila final.
- DELETE cuenta borrados confirmados de claves seleccionadas del snapshot; no
  prueba que la fila siguiera existiendo al llegar la solicitud. La política debe permitirlo.
- MERGE cuenta acciones que modifican, no filas del join ni cláusulas no-op.
- El conteo final aparece tras EOF finito y terminación exitosa. La entrada continua
  confirma lotes durante ejecución, sin conteo final antes de EOF. Un error terminal
  no se sustituye por un lote exitoso con conteo parcial.
- Los modos ACK soportados son `all`/`-1` y `1`. ACK=1 no ofrece la misma garantía
  de replicación que `all`; se conserva la política en diagnósticos. ACK=0 se rechaza.

### Ruta de lotes Arrow

El sink log usa la ruta de lotes Arrow del cliente para entradas finitas y continuas.
Particiones mixtas usan layouts efectivos; grupos contiguos comparten respaldos,
los intercalados usan Arrow take y los slices por bytes conservan backpressure.
Leases de entrada y cast/gather usan el pool de sesión real hasta liberar el último
buffer. Ver [inventario de escritura Arrow](arrow-write-inventory.md) para límites
de copia, codificación por filas requerida en KV, estimaciones y verificación.

### Conocimiento independiente de operaciones

La aceptación conjunta de INSERT continuo está documentada en
[aceptación de escrituras streaming](streaming-write-acceptance.md): fuente Fluss
real hacia sinks log/KV old2/new3, ACK dispersos/idle/cancel, independencia de
pares privados y diferencias entre duplicación de append y conteos de operaciones/
claves de upsert PK al replay explícito. Casos Docker nativos controlados cubren
errores, backpressure y cancelación en entrada/buffer/ACK.

Se usan identidad de ejecución/lote y conteos acumulados de operaciones confirmadas.
Como mínimo se distinguen estos hechos; los nombres públicos Rust son detalle de implementación:

| Hecho | Conclusión permitida |
| --- | --- |
| No encolado | Este trabajo se rechazó antes de enviarse; trabajos anteriores son un hecho separado |
| Enviado, resultado desconocido | Puede haberse aplicado; ACK perdido/timeout no identifica qué filas lo fueron |
| Confirmado | Se conoce la confirmación según la política ACK declarada para esas operaciones |
| Entrada/ejecución terminó | Estado terminal separado: completo, fallido o cancelado; indica si observaciones son incompletas |

Errores y conocimiento de la aplicación son ejes separados. Ejemplos:

- Lote 1 con ACK y lote 2 con ACK perdido: lote 1 sigue confirmado; resultado de
  lote 2 desconocido, no necesariamente sin efecto ni confirmado.
- Error de enqueue tras algunas filas: no se declara todo el lote como no enviado.
  Se expone solo la granularidad que el cliente establezca.
- Todos los lotes con ACK y luego error de limpieza: se conservan confirmaciones
  conocidas; el error de limpieza no las vuelve desconocidas ni se oculta.
- Error de entrada tras ACK previos: falla la sentencia y conserva esos hechos.

Se exponen observaciones/resúmenes opcionales y acotados junto a resultados DataFusion,
reutilizando handles de resultados del cliente y primitivas async estándar. No se
retienen filas indefinidamente ni se infieren efectos por fila a partir de un único
error agregado de `flush()`. Se detecta rezago y se conservan límites inferiores
acumulados conocidos; no hay framework genérico de eventos, retry implícito de
sentencias, compensaciones ni bucle de conciliación. Ambos providers exponen
`subscribe_writes()` para eventos acotados `FlussWriteProgress` y snapshots terminales
`FlussWriteSummary`. El flush nativo exitoso de lote confirma antes de EOF; un lote
fallido intentado sigue conservadoramente incierto. Las métricas `DataSink` nativas
siguen las vidas de ejecución/propietarios. Conteo/error original se conserva. Ver
[contrato de observación de escritura](write-observation-contract.md) para granularidad,
rezago/completitud, métricas y límite de conciliación del motor.

### Límites de DELETE/MERGE

DELETE usa filtros exactos de DataFusion sobre un snapshot KV finito y el writer
por clave existente. Sin WHERE elige todas las filas visibles; sin coincidencias
devuelve cero. DataFusion 55.1 rechaza durante resolución SQL predicados cualificados
con alias del destino DELETE (por ejemplo `DELETE FROM state AS s WHERE s.id = 2`).
Use el nombre de tabla o columnas sin cualificar; el conector no reescribe alias.
MERGE sí admite alias de origen/destino y los predicados DELETE sin cualificador
funcionan. El core nativo incorpora protección upstream de entrada vacía/restricciones:
FALSE/NULL optimizado devuelve cero; planes de join con subquery y DELETE LIMIT se
rechazan antes de llamar al provider. Planes probadamente vacíos no ejecutan sink ni
emiten observaciones de escritura Fluss. Ver [contrato DELETE](delete-contract.md)
y [proveniencia vendor](../vendor/README.md) para límites de selección/conteo/
concurrencia verificados. Se rechazan políticas `ignore`/`disable` y el ignore
implícito para políticas merge-engine aplicables. Cambios concurrentes pueden
invalidar la selección prevista; no existe compare-and-delete ni aislamiento global.
El servidor fijado rechaza ALTER in-place de `table.delete.behavior`; las pruebas
verifican ese fallo explícito, no una mutación de política supuestamente soportada.

MERGE usa semánticas join/filter/CASE de DataFusion y lógica NULL SQL; prevalece
la primera cláusula elegible. Acciones soportadas: UPDATE/DELETE para coincidencias,
INSERT sin coincidencia y acciones admitidas NOT MATCHED BY SOURCE. La fuente debe
ser finita. Se rechaza MERGE SQL en tablas con merge-engine nativo configurado:
upserts ACKed first-row/versionados/agregados no significan reemplazo ordinario de
fila; capacidades y planificación/ejecución hacen cumplir ese límite. INSERT debe
proveer todas las columnas destino; UPDATE no cambia PK/claves de partición. Se
rechazan acciones repetidas que modifican una misma PK, incluso entre lotes, pero
detectar un duplicado posterior no revierte ACK anteriores. No hay atomicidad de
sentencia, resolución de conflictos entre writers ni reparación automática. La
acción elegida NOT MATCHED usa upsert nativo, no INSERT condicional, y UPDATE por
predicado sobre snapshot no es CAS. El rechazo posterior por duplicado conserva ACK
anteriores. Ver [contrato MERGE](merge-contract.md) para semántica verificada de
acciones/NULL/precedencia, recursos, duplicados y concurrencia de writers.

