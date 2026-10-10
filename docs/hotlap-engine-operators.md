# Operadores y snapshots del motor Hotlap

Detalle técnico del [motor Hotlap](hotlap-engine.md).

## 1. Operadores

`crates/hotlap-engine/src/ops/`:

| Operador | Implementación | Semántica |
|---|---|---|
| `Filter` | `arrow::compute::filter_record_batch` | Filtra columnas de datos y `diff` juntos, de modo que las retracciones sobreviven si su fila pasa el filtro. `Predicate::Cmp`/`And`/`Or`/`Not`/`IsNull` con lógica de tres valores (Kleene). |
| `Project` | `arrow::compute` (`ArrayRef::clone`) | Selecciona/reordena columnas compartiendo arrays; O(nº de columnas). Conserva `diff`. |
| `GroupAggregate` | Claves `arrow::row` y reducción incremental | Mantiene `clave → acumuladores` para `count`/`sum`/`min`/`max`/`avg`; actualiza **solo claves tocadas** por el delta (O(delta), no O(keyspace)). El estado admite retracciones (pesos negativos): `count` es `i64`, `sum` entero `i128` con `checked_add`/`checked_sub` (detiene ante overflow); `sum`/`avg` float guardan suma finita y multiplicidades separadas de `NaN`, `+∞`, `-∞`, permitiendo retraer un especial y recuperar parte finita (`avg` conserva total de no nulos). Al materializar, semántica IEEE: cualquier `NaN` o mezcla `+∞`/`-∞` da `NaN`; un único signo infinito domina; `avg` con infinitos es infinito. Retraer un especial de más es error; un delta inválido se valida por completo antes de cambiar estado, incluida conversión a `Int64` de `sum`, así que `apply` rechazado deja filas/sumas/extremos intactos. `min`/`max` no son invertibles: cada clave conserva multiconjunto `valor→cuenta` (`OrderedMultiset`, `BTreeMap` con `Ord` propio; floats vía `f64::total_cmp`); al retirar extremo hasta cero recalcula el siguiente; exceso de retracción satura a cero. Emite changelog `(clave..., aggs...)` con `diff` firmado: al llegar a cero retrae fila vieja; clave nueva inserta; cambio retrae valor anterior e inserta nuevo. La emisión compara la fila de salida materializada, no multiplicidad interna, y compara extremos actuales `min`/`max`; por ello requiere un sink que soporte retracciones cuando el plan puede producirlas. |
| `Join` (inner equi) | Claves `arrow::row` en ambos lados | Cada lado acumula en `KeyedArrangement`; cada `apply` evalúa **solo claves tocadas** y emite el cambio con `Δ(L×R) = ΔL×R_prev + L_prev×ΔR + ΔL×ΔR` (estado `_prev` retenido por clave). Coste O(Σ claves tocadas `|ΔL|·|R| + |L|·|ΔR|`), independiente del espacio de claves no tocado. Salida `left ‖ right`; multiplicidades multiplicadas. |
| `TumbleCount` | Ventana tumbling sobre event-time | Cubetas abiertas en `BTreeMap` por `window_start`; `ws = (event_ts / size) * size`. Emite cada ventana **una vez al cerrarse** con diff `+1` (append-only) y la libera; filas bajo watermark previo se cuentan como tardías y se descartan. Un plan compatible de ventana final puede usar un sink append-only. |

El grafo de vista compone los nodos. En un `Join` cuyas fuentes aún no tienen
esquema, el lado pendiente queda en buffer hasta que ambos esquemas estén disponibles.

## 2. Snapshot del estado agregado

El estado retenido es **serializable** (`crates/hotlap-core/src/snapshot/`): el
snapshot es un contenedor serde (`EngineSnapshot`, versión
`ENGINE_SNAPSHOT_FORMAT_VERSION`) codificado con **bincode**. El estado de
un `GroupAggregate` es `GroupState` → `key bytes -> GroupEntry`, y cada
`GroupEntry` guarda su multiplicidad de filas y un `AggValue` por agregado:

- `count` → `Count(i64)`;
- `sum` entero → `SumInteger { sum: i128, count: i64 }` (el `count` distingue
  «suma = 0» de «sin valores no nulos») y `sum` float →
  `SumFloat { sum, count, nan, pos_inf, neg_inf }`, donde `sum` acumula solo los
  valores finitos y las otras cuentas llevan las multiplicidades de `NaN`, `+∞`
  y `-∞`;
- `avg` → `Avg { sum: f64, count, nan, pos_inf, neg_inf }` (suma finita + nº
  total de no nulos + las mismas multiplicidades);
- `min`/`max` → `Min`/`Max(OrderedMultiset)` (el multiset `valor→cuenta`).

Los extremos se serializan con su multiset completo, de modo que una restauración no
pierde la capacidad de retraer el extremo actual. Los cambios semánticos de
un campo exigen subir la versión de formato; los lectores rechazan cualquier
otra. La versión se lee del **prefijo del cuerpo** antes de decodificar el
layout, de modo que un snapshot anterior cuyo layout ya no decodifica se rechaza
como `Unsupported` (no corrupción) y la recuperación lo trata como fatal, sin
lector antiguo ni migración.
