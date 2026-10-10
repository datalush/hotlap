# Contrato y verificación de DELETE KV nativo

DELETE usa `UpsertWriter::delete` y operadores DataFusion existentes para seleccionar
/filtrar claves desde snapshots KV finitos por bucket. No tiene parser SQL,
evaluador de filas, coordinador transaccional ni transporte DELETE propio.

## Selección y conteo

- Sin `WHERE`, selecciona todas las filas visibles a los scans snapshot. Si no hay
  claves coincidentes, devuelve conteo estándar UInt64 cero.
- Predicados siguen lógica NULL SQL de DataFusion: `value = NULL`, `FALSE`, `1 = 2`
  y una conjunción NULL imposible afectan cero filas, no todas.
- UDF, filtros residuales exactos, OR/IS NULL y qualifiers de tabla usan planner y
  registro de funciones del llamador. Predicados sin calificar sobre alias destino
  funcionan. DataFusion 55.1 rechaza durante resolución SQL predicados calificados
  por alias destino; el conector no reescribe alias.
- Conteo es número de **operaciones de borrado de claves seleccionadas confirmadas**,
  no filas netas removidas. Si otra sentencia elimina una clave después de selección,
  cuenta si ACK de su request llega. DELETE posterior cuyo snapshot no selecciona
  claves cuenta cero.
- Codificación/routing PK permanecen nativos. Claves compuestas `(region,id)` con
  mismo id en otra partición son distintas; layouts efectivos old2/new3 se respetan
  tras rescale.
- Logs append-only rechazan DELETE individual. Limpiar filas log mediante otra API,
  retención o administración no equivale a borrado SQL de filas.

Selección snapshot y borrado de claves **no son atómicos ni condicionales**. Un valor
que cumple WHERE puede reemplazarse tras selección y el borrado nativo de clave puede
eliminar ese reemplazo. Cada bucket abre su propio snapshot; no hay transacción común
entre buckets/particiones. El motor posee política de conflictos, reintentos/conciliación
trabajo y requisitos de aislamiento.

## Políticas de tabla

El conector comprueba metadatos efectivos al planificar, ejecutar DELETE y antes de
enviar lotes. Solo acepta `table.delete.behavior=allow`. Rechaza `ignore`/`disable`,
en vez de convertir ACK de petición ignorada en conteo SQL exitoso.

Tablas PK normales permiten por defecto. Motores merge `first_row`/`versioned`/
`aggregation` configurados usan `ignore` por defecto cuando no se declara política;
el servidor materializa propiedad efectiva para descriptores admitidos. `first_row`/
`versioned` no permiten descriptor `allow`; `aggregation` con allow explícito sigue
validación del servidor. Conducta efectiva desconocida/no-allow se rechaza
conservadoramente.

Servidor fijado **no** admite ALTER in-place de `table.delete.behavior`. Prueba live
verifica rechazo y que plan allow original sigue válido; no simula transición que el
servidor no soporta. Capacidades cacheadas son descriptivas, no autorización ni prueba
de metadatos futuros. Permisos y matriz completa de failover están en
[verificación de fallos nativos](native-failure-verification.md).

## Ejecución parcial

Contrato de observación de escritura aplica con `FlussWriteOperation::Delete`. Lotes
completos con ACK anteriores a fallo permanecen confirmados. Lote intentado posterior
sin flush agregado exitoso sigue incierto; SQL devuelve error original y no conteo
parcial exitoso. Algunas o todas las claves inciertas podrían borrarse tras timeout/
cancelación. No hay inserts compensatorios ni rollback. Ver
[contrato de observación de escritura](write-observation-contract.md).

## Backport requerido de DataFusion nativo

Regresión nueva `value = NULL` reveló que DataFusion 55.1 convertía `EmptyRelation`
optimizada en vector de filtros vacío entregado a `delete_from`, que para provider
significa borrado sin condición. Borró las tres filas fixture aisladas, no cero. La
información se pierde antes del hook del conector, por lo que solo provider no puede
distinguirlo de DELETE legítimo sin WHERE.

Con autorización, workspace aplica patch al core **publicado DataFusion 55.1** y
conserva versiones Arrow 59/catalog/session/FFI:

- Backport PR upstream [24657](https://github.com/apache/datafusion/pull/24657),
  commit `2306a4b7599dc88490c0b39f082b4cdf554a5fb9`.
- Entrada DELETE/UPDATE probadamente vacía devuelve cero sin invocar provider.
- Joins/restricciones optimizadas no soportadas se rechazan en vez de perder selección.
- Rechazo genérico fail-closed de `Limit` sigue manejo de restricción no soportada;
  DELETE LIMIT no está implementado. Planes subquery IN/EXISTS con join no son
  soportados por hooks que solo filtran.

Única fuente upstream modificada: `vendor/datafusion-55.1.0/src/physical_planner.rs`.
Checksum/aviso/proveniencia del archivo y condiciones de retiro constan en
[README vendor](../vendor/README.md). Fijar directamente workspace upstream fusionado
exigiría Arrow 60; este backport evita esa migración ajena.

Sentencia optimizada y probadamente vacía nunca invoca sink nativo: devuelve conteo
SQL cero y no emite eventos Fluss de observación escritura. Distinto de sink ejecutado
que recibe snapshot vacío y termina con cero recibidas/confirmadas. No inventar ID de
ejecución sink para plan no-op upstream.

## Evidencia registrada

- `tests/delete_planner.rs`: tres pruebas nativas independientes de transporte con
  MemTable: DELETE vacío, UPDATE vacío y restricciones subquery/LIMIT rechazadas;
  verifican filas preservadas y conteos cero/normal.
- `tests/write_sql.rs`: cinco pruebas native-sni. Casos ampliados cubren FALSE/NULL/
  contradicción, sin WHERE/cero, UDF/OR/NULL/alias, rechazo append-only, PK compuesta
  y preservación datos old2/new3. Política cubre default/allow/ignore/disable/
  first_row, ACK ignorado sin borrar fila, rechazo SQL/capacidades, ACK de clave ausente
  y ALTER no soportado.
- `tests/write_pressure.rs`: matriz Docker propia añade carreras snapshot-vs-upsert y
  DELETE concurrente deterministas; ambas devuelven conteo ACK de clave seleccionada
  uno. Luego confirma primer lote snapshot antes de ACK posterior pausado, verifica
  SQL fallido con resumen DELETE confirmado/incierto, filas restantes y liberación pool.

Pruebas funcionales usan DEBUG/ocho jobs. Bindings pertenecen a migración separada.
No se reemplazó ruta cliente/conector ni se retuvo como fallback: fix pertenece al
planner nativo genérico que originalmente perdía selección.

Evidencia final registrada con dependencia parcheada: **5 pruebas SQL native-sni,
3 regresiones planner genérico y matriz Docker completa con carreras/ACK parcial**.
Clippy core all-targets/all-features `-D warnings`, formato paquete, formato fuente
vendor upstream y `git diff --check` pasaron. Comparación archivo confirmó que solo
`physical_planner.rs` difiere en paquete vendorizado. Se limpiaron dos pares exactos
de tablas que dejaron aserciones fallidas; fixtures Docker propios fueron derribados.
No se reconstruyeron bindings.
