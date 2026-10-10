# Evidencia histórica del laboratorio nativo

Pruebas de integración registradas en el laboratorio aislado native-sni. El alcance
y la configuración vigente se resumen en
[preparación para producción](production-readiness.md).

## Verificado en el laboratorio nativo-sni aislado

- Particiones con layouts mixtos: las particiones existentes de dos buckets
  siguen legibles al cambiar el default de tabla a tres. La prueba escribe en el
  bucket 2 de la partición nueva y la lee mediante ambos providers SQL.
- Un writer Rust existente escribe en particiones antiguas y nuevas; búsquedas
  puntuales hallan claves KV usando el conteo de routing de cada partición.
- Esperar una escritura informa su ACK. `flush()` informa ACK fallido aunque
  llegara antes de iniciar flush; cancelarlo libera el modo flush.
- Paginación KV, actualizaciones, borrados, evolución de esquema y cancelación.
  Perder el líder tabletserver invalida snapshot KV abierto; un scanner *nuevo*
  funciona tras recuperación, no continúa sobre otro snapshot.
- El pool DataFusion contabiliza colas decodificadas y respaldos retenidos de
  fuente tras admisión. Leases siguen al último propietario del buffer Arrow,
  incluidos clones/slices/proyecciones retenidos tras cancelar el stream. Pools
  restrictivos fallan explícitamente; asignaciones crudas/transitorias de decode
  quedan fuera de protección previa y el contador no limita RSS total. Un
  consumidor KV detenido no solicita otra página.
- Logs batch capturan offsets retenidos más antiguos y más recientes. Si retención
  rebasa inicio capturado antes de suscribirse, lectura falla. Una prueba unitaria
  inyecta respuesta fuera de rango *después* de datos de otro bucket y comprueba
  que Arrow batch reader falla en vez de ocultar error.
- La fuente streaming se declara no acotada y espera durante polls idle; un log
  aislado en native-sni entregó filas añadidas tras iniciar consulta, sin EOF entre
  escrituras. Eventos de entrega incluyeron offsets siguientes e ID de ejecución
  correctos. Pausar consumidor mantuvo estable reserva compartida DataFusion de
  8 MiB. Drop libera cola; buffers de salida retenidos siguen cobrados hasta soltar.
  Scan batch explícito reanudó desde offset 1 sin repetir fila 0; offset inválido
  falló. Añadir partición durante scan streaming particionado produjo error de
  topología explícito. El stream filtrado usó `batch_size=1`; coalescing default
  de `FilterExec` puede retrasar resultados pequeños no terminantes (ver semántica
  de lectura). No es checkpoint de motor, changelog KV continuo ni perfil sostenido.
- Esquemas log evolucionados leen filas históricas sin empujar campos nuevos a
  proyección del servidor. Provider DataFusion nuevo devuelve NULL para filas
  antiguas; plan antiguo conserva su esquema o falla. Tras borrar/recrear tabla
  con mismo nombre, planes log/KV antiguos rechazan nueva identidad. Ejecuciones
  concurrentes de una fuente física con TaskContexts distintos completan.
- Prueba failover ignorada en native-sni reinicia solo coordinador activo con
  stream log abierto. Stream original completa todas las filas capturadas o falla;
  al volver cluster a Green, scan nuevo devuelve 512 filas. Otra prueba rechaza
  CA TLS no confiable sin mostrar contraseña SASL.
- Presupuesto de ejecución (default: 16.384 pares partición/bucket seleccionados)
  rechaza scans mayores, no omite buckets; `with_max_assigned_buckets` configura
  deliberadamente un límite superior.
- SQL `INSERT INTO` agregó filas log, hizo `INSERT ... SELECT` entre tablas Fluss
  y upsert de claves KV. Conteos reflejaron entradas ACKed y scans posteriores
  verificaron valores reales. Dos inserts concurrentes usaron writers separados.
  Plan físico INSERT rechazó escribir en tabla borrada/recreada bajo misma ruta.
  Lotes Arrow mixtos rutearon layouts antiguos de dos y nuevos de tres buckets
  para log/KV. Entrada SQL 4 MiB completó con buffer writer Fluss 2 MiB (routing
  en blocking pool Tokio, sin bloquear sender). INSERT SELECT desde log no acotado
  entregó y confirmó cada lote antes de EOF; cancelar consulta impidió que escrituras
  posteriores llegaran a destino. Escrituras parciales no se revierten ni informan
  como operación plenamente exitosa.
- Endurecimiento posterior del árbol Rust cobra lotes sink retenidos al pool de
  consulta y comparte plazo enqueue/ACK. Entrada 4 MiB sigue completando con
  buffer writer 2 MiB, liberando reservas; pool DataFusion de un byte rechaza
  INSERT antes de escribir. Pruebas rechazan ACK fire-and-forget y NULL en columnas
  requeridas. DELETE KV SQL pasó filtros exactos, cero coincidencias, todas las
  filas y borrado particionado tras rescale; políticas ignore configuradas/implícitas
  se rechazan. Matriz Docker nativa final verifica cancelación durante ACK bloqueado
  y buffers saturados, writers independientes, preparación/metadatos temporizados y
  recuperación del pool. Ver [presión de escritura](write-pressure-verification.md).
- MERGE Rust pasó fuente VALUES finita con UPDATE/DELETE/INSERT, cláusula falsa/no-op,
  precedencia de primera cláusula, borrado NOT MATCHED BY SOURCE y rechazo explícito de claves
  modificadoras duplicadas sin aplicar el lote actual. Compone join/filter/CASE de
  DataFusion; no añade evaluator SQL. Cambios PK, lista INSERT incompleta y fuentes
  MERGE no acotadas no se soportan. Casos finales de duplicado tardío/concurrencia/
  ACK parcial y propiedad source/key/scratch pasan; [contrato MERGE](merge-contract.md)
  registra límites nativos no-CAS/no transaccionales.
- [Aceptación final de INSERT continuo](streaming-write-acceptance.md) usa fuente
  Fluss real hacia destinos log/KV old2/new3, ACK disperso/idle/cancel y replay
  explícito desde earliest. Suite SQL final tiene ocho casos opt-in; fallos Docker
  controlados complementan entrega/routing de fuente real.
- [Observaciones de escritura](write-observation-contract.md) conservan ACK previos
  y clasifican conservadoramente lotes intentados sin confirmar. El contrato
  [DELETE](delete-contract.md) incluye backport genérico DataFusion nativo para
  entrada vacía/restricciones; su proveniencia está en [vendor/README.md](../vendor/README.md).

La ruta de planificación Rust nativa pasó 15 pruebas unitarias y cuatro
integraciones `write_sql` reales en debug/ocho jobs. Grafos auxiliares DELETE/MERGE
invocan planner del llamador; DELETE usa su registry UDF y MERGE fuente MemTable de
tres particiones con alias. Distribución sink nativa consume cada partición sin
coalesce del conector; mismo plan físico INSERT ejecutado dos veces verificó seis
operaciones. Capacidades exponen vista inmutable metadatos/modo, no autorización.
Ver [resolución de planificación nativa](rust-implementation-history-details.md#resolución-de-planificación-nativa--2026-10-04)
y limitación upstream de alias DELETE. No certifica runtime consumidor ni acepta
el sistema completo.
