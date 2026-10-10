# Semántica de escritura SQL en los providers Fluss

Detalle de escrituras que acompaña la
[semántica de lectura](reading-semantics.md).

`INSERT INTO` es operación sink en ambos providers. Esquema destino e IDs
tabla/esquema se verifican antes de escribir y por lote de entrada. DataFusion
entrega stream asíncrono de lotes Arrow a writer aislado por sentencia. Cliente
Fluss existente agrupa/rutea cuando lotes mezclan particiones con conteos históricos
distintos. Bloques log compatibles usan vistas/slices; grupos intercalados usan
Arrow gathers; KV conserva codificación por filas requerida. INSERT log agrega;
INSERT KV hace upsert completo. Claves duplicadas de una entrada cuentan dos filas
enviadas; entradas paralelas sin orden no garantizan ganador. Sink espera ACK por
lote aunque entrada no termine. `count` final solo está disponible tras EOF.
Cancelación/fallo posterior deja filas confirmadas; no promete rollback entre lotes,
checkpoint source/sink ni exactly-once. Presupuesto de buffers y política ACK son
de Fluss Config; opciones sink limitan espera ACK e intentos.

Sink cobra buffers retenidos de entrada/cast/gather, codificación/transporte nativo,
metadatos routing y scratch claves/filas al pool real de ejecución. Owners mantienen
reservas durante vida de worker/buffer/frame, incluso tras cancelación. Cargos de
entrada/operador pueden solaparse conservadoramente; slots de cola cliente tienen
limiter propio. Preparación/metadatos son finitos; deadline enqueue/ACK compartido
cubre lote, no input idle. Cancelación aborta writer dedicado y despierta productores
bloqueados; requests enviados aún pueden confirmarse. Conteos requieren
`writer_acks=all`, `-1` o `1`; ACK fire-and-forget se rechaza. Columnas destino
requeridas se validan por NULL antes de enqueue.

Rust `DELETE FROM kv WHERE ...` combina scan KV finito con evaluación exacta del
predicado DataFusion y envía claves seleccionadas al writer delete Fluss. Sin
filtro selecciona todas las filas snapshot; sin coincidencias devuelve cero.
Conteo es operaciones delete confirmadas sobre filas seleccionadas, no prueba de
cuántas existían al llegar al server. Cambios concurrentes pueden verse afectados
por borrado previamente seleccionado: no hay escritura condicional ni aislamiento
de sentencia. Política de tabla debe permitir borrado; `ignore`/`disable`, incluido
ignore implícito con merge engine configurado, se rechaza antes del envío. Core
DataFusion nativo incorpora backport de protección para DELETE/UPDATE vacíos y
restricciones de filas no soportadas; ver [contrato DELETE](delete-contract.md)
y proveniencia vendor.

El árbol Rust planifica MERGE de fuente finita con joins DataFusion y expresiones
CASE ordenadas. Predicados usan lógica SQL de tres valores; solo primer WHEN
elegible modifica cada fila unida. UPDATE/DELETE matched, INSERT unmatched y NOT
MATCHED BY SOURCE usan writer upsert/delete existente. INSERT requiere todas las
columnas destino; cambios de PK/claves partición y fuentes MERGE no acotadas se
rechazan explícitamente. Sink detecta acciones modificadoras repetidas por PK entre
lotes y cobra conjunto de claves codificadas a memoria DataFusion. Duplicado en lote
posterior no revierte modificaciones previamente confirmadas. Selección snapshot
sigue por bucket, sin escrituras condicionales ni aislamiento global. Tablas con
merge-engine nativo configurado no dan reemplazo ordinario de filas y se rechazan
para MERGE SQL. Ver [contrato MERGE](merge-contract.md) para límites y evidencia.

`FilterExec` de DataFusion puede agrupar lotes pequeños hasta `batch_size` de sesión.
Un batch_size menor puede favorecer latencia de streaming disperso, con coste de
agrupación; fuente no añade otro filtro SQL. Aceptación nativa source/sink y
semántica replay están en [aceptación de INSERT continuo](streaming-write-acceptance.md).
