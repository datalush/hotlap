# Hotlap — `hotlap-engine`: motor incremental columnar

- Fecha del registro: 2026-10-08
- Estado histórico: implementación documentada al 2026-10-08.
- Alcance histórico: motor columnar, contrato `IncrementalCore`, operadores e integración.

> El [núcleo incremental](hotlap-incremental-core.md) describe el contexto técnico
> y el contrato vigente de actualización por delta.

## 1. Arquitectura

La decisión, las crates y la frontera del core se describen en
[arquitectura del motor](hotlap-engine-architecture.md).

## 2. Tiempo: epochs, fronteras y retracciones

- **Watermark** (`crates/hotlap-engine/src/time.rs`): `max(event_ts) - lag`,
  acotado a cero y **monótono**. Cada observación lo avanza al menos a
  `max_ts - lag`; nunca retrocede.
- **Frontera** (`Frontier`): por entrada, tiempo mayor anunciado como completo.
  Un epoch queda asentado cuando todas las entradas lo superan; el
  mínimo (`Frontier::min`) es el watermark de la vista.
- **Datos tardíos**: `EngineCore::push` descarta **inserciones** (`diff > 0`) por
  debajo del watermark vigente y las cuenta en `late_dropped`. Una **retracción
  nunca se descarta**: aplicarla siempre es necesario para no corromper el estado
  en operadores posteriores.
- **Retracciones**: se representan como `diff` negativo. Los arrangements suman
  los deltas de cada `(clave, payload)` y olvidan las entradas cuya suma llega a
  cero; los operadores de conteo/ventana propagan el changelog correspondiente
  (retraer el valor viejo e insertar el nuevo).

## 3. Operadores

La semántica incremental, el estado agregado y el snapshot de operadores se
describen en [operadores del motor](hotlap-engine-operators.md).

## 4. Integración en los bordes

`hotlap-connectors` y `hotlap-sql` usan **Arrow en sus fronteras**: convierten
lotes `RecordBatch` al contrato Z-set, gestionan su columna `diff` y construyen
`Hotlap::open_with(EngineCore::new())`.

## 5. Validación incremental

- El **oráculo** es la **recomputación completa** con un cálculo por lotes
  independiente: cada operador y combinación se contrasta en modo incremental y
  batch, con retracciones y varios epochs.
- La corrección incremental (retracciones, fronteras, consolidación, joins) es
  **nuestra**; por eso la batería diferencial es exhaustiva.

## 6. Límites y fuera de alcance

- **Coste del join**: se procesan solo las claves tocadas por los deltas, no el
  producto completo `|L|·|R|`. La recomputación completa se usa únicamente como
  **oráculo** en los tests que comparan operadores independientes.
- **Estado sin poda (GC)**: coincide con el estado corriente, sin historia; los
  arrangements, las cubetas de ventana y las salidas acumuladas **no se podan**.
  No hay recolección de basura ni gestión avanzada de datos tardíos.
- **`min`/`max` guardan todos los valores distintos**: al no ser invertibles, el
  multiset por clave crece con el nº de valores distintos vivos, no con el
  resultado. Una poda exigiría otra estructura. Un delta
  **no clona** el multiset: materializa solo la fila de salida de las claves
  tocadas y la compara con la anterior, así que su coste depende del delta y no
  de la cardinalidad de la clave.
- **Agregados con ventana**: `TumbleCount` solo computa `count(*)`; `sum`/`min`/
  `max`/`avg` por ventana quedan fuera de alcance (hop/sliding).
- **Un solo worker**: versión 1 sin exchange, sin *spill* y sin persistencia.
- **IR limitado**: solo operadores descritos en documento enlazado; tipos/agregados fuera del IR se
  rechazan con `Unsupported`. Sin hop/sliding/session.
- **Pruebas incrementales**: evidencia de corrección incluye
  equivalencia *incremental ≡ recomputación completa*; no es una prueba formal.
  Pruebas: `arrange::incremental_matches_full_recompute`,
  `ops::group_count_changelog_consolidates_to_final_counts`,
  `join::join_matches_full_recompute_with_retractions`,
  `window::changelog_matches_full_recompute_across_windows`,
  los tests de paridad incremental del workspace.
