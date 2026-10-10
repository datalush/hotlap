# Contrato y verificación de MERGE KV nativo

MERGE compone operadores nativos DataFusion Full join, predicados exactos, CASE y
proyección; después usa writer KV existente de upsert completo/borrado de claves.
El grafo auxiliar SELECT atraviesa planner/optimizador de Session suministrada.
`InputPlan` adapta fuente física suministrada mediante TableProvider; no es otro
motor SQL, evaluador de filas, scheduler ni sesión reconstruida.

## Semántica admitida

- La fuente debe ser finita. Se admiten `VALUES` y otros providers/planners si su
  fuente física informa acotación; se rechaza entrada continua log/CDC.
- Tablas PK ordinarias admiten UPDATE/DELETE matched, INSERT unmatched y
  UPDATE/DELETE NOT MATCHED BY SOURCE admitidos. Destinos append-only rechazan MERGE.
- Tablas con `table.merge-engine` configurado se rechazan para MERGE SQL. Upserts
  first-row/versioned/aggregation pueden ignorar/versionar/agregar en vez de
  reemplazar fila, así ACK no demuestra semántica UPDATE SQL ordinaria.
  Capacidades registran restricción y planificación/ejecución la revalidan.
  INSERT/upsert nativo conserva conducta de la tabla subyacente.
- Condiciones siguen lógica NULL SQL DataFusion; solo TRUE selecciona cláusula.
  Predicado NULL inicial no suprime cláusula posterior aplicable.
- Gana primera cláusula elegible. Presencia matched/solo-destino/solo-fuente se
  representa aparte de NULL; payload nullable no implica fila ausente.
- UPDATE conserva columnas destino no asignadas y no asigna columnas PK/partición,
  aunque expresión parezca mantener valor actual.
- INSERT suministra cada columna destino una sola vez, con reordenamiento admitido.
  Se rechazan columnas faltantes/duplicadas/desconocidas o PK requerida NULL; no se
  inventan valores nullable/default para permitir INSERT parcial.
- Conteo son operaciones modificadoras confirmadas, no filas join/cláusulas/filas
  fuente ni cambios netos. Filas no-op no cuentan ni participan en detección duplicados.

## Coincidencias múltiples y aplicación parcial

La detección de duplicados sigue PK codificada de **acciones modificadoras** durante
toda ejecución finita. Dos filas fuente que coinciden con un destino se admiten si
solo una crea acción modificadora; duplicados no-op no afectan. Dos acciones
modificadoras para una misma PK se rechazan, en el mismo lote o en lotes posteriores.

La validación de duplicados en el mismo lote termina antes de enqueue. Un duplicado
posterior no revierte ACK anterior. Caso registrado de duplicado tardío:
confirmed=1, rejected_before_enqueue=1, uncertain=0, SQL fallido y primer valor
nuevo aún almacenado. No restaura valor original ni reintenta sentencia.

Otros fallos transporte/ACK siguen contrato de observación: lotes confirmados siguen
conocidos; lote intentado sin confirmación flush completa queda incierto. No se emite
conteo parcial exitoso. Ver
[write-observation-contract.md](write-observation-contract.md).

## Concurrencia y propiedad

La selección usa snapshots destino finitos por bucket, no transacción global ni
compare-and-set en writer. UPDATE concurrente posterior a selección puede ser
sobrescrito aunque predicado snapshot ya no coincida con valor actual. Fila fuente
not-matched puede colisionar con PK insertada concurrentemente; upsert nativo de fila
completa la reemplaza, no implementa INSERT condicional ni transacción de unicidad.
El motor posee política conflicto/replay.

Routing PK, claves compuestas/partición y layouts antiguos/nuevos efectivos son
responsabilidad del cliente. MERGE no migra claves entre particiones. Planes físicos
fuente y reglas reset/reejecución de operadores nativos son contrato DataFusion.

Estado claves duplicadas y scratch `RowConverter` reservan en pool suministrado;
scratch usa columnas PK seleccionadas, no payload no-clave. Codificación fila KV,
buffers y owners RPC conservan reservas separadas. Métricas muestran vida scratch
merge-key/KV. Query fallida libera esos owners, pero plan físico `HashJoin` retenido
externamente puede poseer buffers build-side snapshot destino: leases source siguen
cobrados hasta drop del plan/estado. No fuerce reservas source a cero solo para
satisfacer aserción de limpieza sink.

## Cobertura registrada

- Evidencia `write_sql` existente: UPDATE/DELETE/INSERT en una sentencia, precedencia
  primera cláusula, no-op, DELETE NOT MATCHED BY SOURCE, duplicados mismo lote,
  MemTable externa de tres particiones/planner propio, layout compuesto `(region,id)`
  old2/new3 y rechazo de update PK/partición.
- Nueva `merge_preserves_null_logic_precedence_and_rejects_unsupported_variants`:
  predicado NULL seguido de UPDATE, payload nullable, DELETE/UPDATE solo destino,
  reordenar columnas INSERT, preservar columnas no asignadas, duplicado no-op/una
  coincidencia activa y rechazo de update PK, INSERT parcial, PK NULL, destino
  first-row y fuente no acotada. Comprueba conteos, resumen y filas almacenadas.
- Nueva `merge_duplicate_in_later_batch_preserves_earlier_ack_and_releases_state`:
  lotes de una fila exponen primer ACK antes de rechazar duplicado posterior; resumen
  terminal conserva ACK, valor almacenado demuestra no rollback y pool se libera dentro
  de observación acotada de cancelación.
- Matriz Docker propia: UPDATE snapshot vs upsert concurrente e INSERT solo-fuente
  vs insert concurrente demuestran conducta nativa sin CAS. Stall ACK posterior
  conserva prefijo confirmado y resultado MERGE incierto, comprueba filas del prefijo
  enviado, estado de claves admitidas y liberación scratch key/KV. Buffers fuente
  destino retenidos por plan físico inspeccionado siguen cobrados hasta drop del plan.

Builds funcionales usan DEBUG/ocho jobs. Esto no implica aislamiento transaccional,
semántica exactly-once, snapshots globales ni soporte de fuentes MERGE no acotadas.
Permisos/failover y aceptación de INSERT continuo se cubren por separado; la
aceptación completa del motor consumidor requiere integración nativa específica.

Verificación registrada: **7 pruebas SQL native-sni, 29 pruebas core y matriz Docker
completa con carreras MERGE/ACK parcial aprobadas**. Clippy core all-targets/all-features
con `-D warnings`, formato de paquete y `git diff --check` pasaron. Las aserciones
verifican propiedad real: cancelar sender es asíncrono y HashJoin retenido externamente
mantiene legítimamente buffers fuente. No se libera lease antes de tiempo ni quedan
contenedores Docker propios de presión.
