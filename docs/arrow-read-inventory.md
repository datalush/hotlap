# Inventario de materialización de lecturas Arrow

Inventario de límites concretos de materialización en la ruta Rust; no afirma
zero-copy universal ni es benchmark de throughput. Leases fuente del núcleo y
planificación nativa siguen siendo contratos en [contrato Rust](rust-contract.md).

## Rutas de datos y disposición

| Límite | Comportamiento anterior/conservado | Cambio o justificación |
| --- | --- | --- |
| RPC/archivo log → lote IPC | Parsear/descomprimir wire y resolver esquema de escritura | Formato/compresión pueden asignar memoria. No hay segundo decoder de protocolo en conector |
| Scanner log → reader acotado/streaming | Lotes individuales con offsets; reader encola y recorta rangos | Recorte usa slices Arrow. Fuente DataFusion nativa no concatena esos lotes |
| Proyección/pruning log con esquema inicial | Proyección solicitada en fuente; poda de lote completo | Filtros SQL residuales exactos siguen siendo necesarios; no duplicarlos en cliente |
| Alineación log con esquema evolucionado | Decode con esquema al escribir, alinear campos destino; faltantes pasan a NULL | Se reutilizan arrays compatibles; representación distinta requiere nuevos nulls/normalización de tipo |
| Preview log `LimitBatchScanner` | Ruta rápida de lote único; varios lotes se concatenan para API de resultado único | No es ruta fuente DataFusion; quitar concat altera semántica API preview |
| Registros wire KV → fila lógica actual | `FixedSchemaDecoder` alinea field IDs originales; antes convertía filas completas al primer acceso | Proyección selecciona ahora campos físicos fuente. Campos no seleccionados se recorren por límites wire comprobados sin convertirlos |
| Fila lógica KV → Arrow | Antes construía todas las columnas y luego proyectaba | Construye solo campos únicos solicitados; reordena/duplica con proyección nativa por referencias Arrow |
| Límite preview KV | Antes construía todas las filas y luego cortaba `limit` final | Selecciona rangos sufijo antes de convertir/construir; framing/schema-ID servidor no cambia |
| Proyección COUNT KV | Antes decodificaba valores completos y los quitaba en conector | Batch readers KV admiten proyección cero columnas, conservan conteo explícito y no construyen columnas de valores Arrow |
| Admisión/salida fuente | Se cobra almacenamiento respaldo original; owners Arrow custom conservan leases | Headers/owners asignan metadatos; payload/null/offset/child conservan punteros/geometría |
| Filtros exactos, casts, sort/agregados DataFusion | Pueden materializar arrays salida distintos | Semántica SQL nativa, no copias arbitrarias del conector; contabilización salida corresponde a operadores |

Rutas cliente relevantes bajo `clients/rust/crates/fluss/src/`:
`client/table/{batch_scanner,reader,kv_scanner}.rs`,
`client/table/scanner/{batches,builder,polling}.rs`,
`row/{fixed_schema_decoder,row_decoder}.rs`,
`row/compacted/compacted_row_reader.rs`, `record/arrow.rs` y
`client/table/read_context_resolver.rs`. Rutas conector:
`crates/fluss-datafusion/src/{scan,kv_scan,resources,filter,log_table,kv_table}.rs`.

## Decisiones de implementación KV

- Usar deserializador compactado existente y sus readers escalares/bytes comprobados.
  Float/double tienen ancho fijo; enteros/date/time usan codificación variable real;
  anchos timestamp/decimal siguen formato/precisión. No inferir ancho por tipo Arrow
  ni introducir parser wire en conector.
- Campos seleccionados fuente se derivan del mapeo existente source→current por
  field ID. Campos actuales ausentes siguen NULL. Proyección builder Arrow mapea luego
  fila actual alineada a campos solicitados, incluido reordenamiento.
- La fila lógica interna conserva posiciones originales por compatibilidad; slots
  no seleccionados no requieren valores convertidos. Esto, schemas/máscaras/builder
  son asignaciones metadatos que la optimización no elimina.
- Columnas duplicadas solicitadas se materializan una vez y comparten `ArrayRef`.
  No hace falta `take`/gather de valores solo para duplicar/reordenar.
- Proyección no valida contenido lógico de valores no solicitados. Por ejemplo, no
  convierte UTF-8 una cadena no seleccionada. Framing registro/esquema sí se verifica;
  longitudes variables recorridas deben caber en payload. Si query selecciona cadena,
  conserva su error de conversión. COUNT solo filas no deserializa valores, análogo a
  contar registros sin validar lógica de cada campo.
- Proyecciones vacías se admiten para lecturas batch PK, no readers log/changelog.
  COUNT sigue recibiendo páginas snapshot/headers de registros; no se añadió RPC count
  servidor, snapshot global ni pushdown filter/limit.
- Schemas Arrow vacíos requieren conteo explícito filas. Builder común conserva
  comportamiento anterior para schemas no vacíos; solo schema vacío requiere conteo.
- Se retiró workaround del conector de proyección vacía post-decode. Cliente devuelve
  ahora schema solicitado. EXPLAIN distingue KV `row_count_only` de fallback log
  `full_rows_for_count` conservado.

## Evidencia registrada

- Pruebas cliente `batch_scanner`: valores seleccionados, sharing `ArrayRef` duplicado/
  reordenado, índices inválidos, COUNT vacío con tres filas/cero bytes Arrow, límites
  y schemas antiguo/nuevo con campos seleccionados ausentes.
- UTF-8 deliberadamente inválido en columna no seleccionada falla conversión completa,
  pero permite leer entero seleccionado con decoder proyectado. Truncar payload wire de
  ese campo aún falla recorrido. Prueba trabajo evitado, no solo resultado final menor.
- Pruebas decoder compactado cubren skip de float/double fijos vs enteros variables,
  además de round trips primitivos/NULL/nested.
- Prueba lease core compara punteros/offsets/longitudes recursivamente, incluso mapas
  validez y buffers hijos nested, en slicing/proyección/retención.
- Consultas SQL reales log/KV acotadas comprueban COUNT en lotes fuente: filas no cero,
  columnas cero y `arrow_decoded_bytes` cero. COUNT SQL salida sigue correcto.
- Integraciones reales snapshot KV/evolución schema y cuatro escrituras SQL cubren
  decoder modificado en SELECT/DELETE/MERGE. Round trips Arrow existentes comprueban
  que cambios row-builder no afectan escrituras.

Evidencia funcional usa DEBUG/ocho jobs. Demuestra reducción estructural de
materialización, no mejora medida throughput/latencia.

## Límites conservados

Bytes red, páginas raw, picos descompresión/poll incompleto y conversión de formato
para valores seleccionados no se eliminan. Admisión ocurre tras decode; perfiles de
recursos verifican límites/pressure acotados. Vistas Arrow compatibles no implican
que toda asignación se cobre al pool fuente.

COUNT log conserva fallback de lectura completa; contrato cliente de conteo solo filas
requiere validar offsets/framing/esquema/pruning, no atajo con conteos header sin
verificar. Normalización esquema log y concat preview son materializaciones explícitas
conservadas. No se añade mmap custom, decoder/runtime alternativo, build Python/FFI ni
registro global de asignaciones.
