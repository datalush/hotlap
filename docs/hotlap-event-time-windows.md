# Hotlap — event-time y ventanas tumbling (SP1c)

- Fecha: 2026-10-06
- Estado: implementado y verificado (SP1c Tasks 1–5)
- Alcance: reloj event-time por input, late-drop, `Plan::TumbleCount`, GC
- Diseño externo: `2026-10-06-hotlap-sp1c-event-time-windows-design.md` (local, fuera del repo)
- Spec padre: `2026-10-06-hotlap-streaming-engine-design.md` §7.6
- Núcleo relacionado: `docs/hotlap-incremental-core.md`

## 1. Modelo híbrido

Dos ejes que no se funden:

1. **Reloj de orden/cierre** = event-time/watermark. Decide el cierre de ventana, el
   descarte de tardíos y el GC.
2. **Modelo de cambio del core** = Z-sets del kernel Arrow-native `hotlap-engine`
   (el spike `differential-dataflow` fue reemplazado).

El event-time es un **atributo de fila** (no el tiempo lógico del core), pero el
watermark **gobierna el descarte de tardíos y el cierre de ventanas** por input. Así la
propagación mínima y el cierre reutilizan la maquinaria del motor sin atar el versionado
del motor a los timestamps de negocio, y no se obliga a que toda tabla tenga tiempo.

## 2. Unidades

- Event-time = `i64` en **milisegundos** (no negativos; los timestamps de Fluss son ms
  desde epoch). Los negativos y los valores no-`I64` se tratan como `0`.
- El timestamp lógico de DD es `u64`. La frontera se escala por `TIME_SCALE` para poder
  registrar avisos a resolución sub-ms de forma segura:
  `TIME_SCALE = 1 << 20` (`session.rs`).

## 3. Modos de reloj

Fijados al construir el dataflow, sin fallback silencioso:

- **epoch** — ningún input declara watermark; comportamiento SP1a/b (`+1` por push).
  No admite ventanas.
- **event-time** — **todos** los inputs declaran watermark
  (`Hotlap::declare_watermark(input, time_col, lag)`). Las ventanas solo aplican en este
  modo.

Declarar un watermark en un circuito epoch-mixto, o construir un `TumbleCount` sobre una
fuente sin watermark, se **rechaza explícito**.

## 4. Avance del watermark

Por push, e independiente del batching:

```
watermark = max(watermark, max(event_ts del lote) - lag)   // monótono, nunca decrece
```

No se fuerza `+1`. Un lote sin filas no mueve el watermark. El descarte de tardíos
compara contra el watermark **previo** al lote.

## 5. Cierre de ventana

`Plan::TumbleCount { input, key, time_col, size }` agrupa por `key` en ventanas tumbling
`[ws, ws+size)` con `ws = (event_ts / size) * size`. La ventana cierra cuando
`watermark >= ws + size` y se emite **una sola vez**, append-only:
`key ++ [window_start, count]`.

## 6. Tardíos

Un evento con `event_ts < watermark` **en la ingesta** es tardío: se descarta y se
incrementa el contador `Hotlap::late_dropped(input)`. Un push rechazado no avanza el
reloj ni cuenta tardíos.

## 7. GC

Al cerrar una ventana, el operador elimina su cubo del mapa de ventanas abiertas: el
estado del operador queda acotado por las ventanas abiertas. El **resultado consolidado
del MV** es append-only y crece con el número de ventanas emitidas; su retención queda
fuera de SP1c.

## 8. Constraint del `Notificator`

`Notificator` dispara **estrictamente pasado** el tiempo, no al tiempo. Para cerrar en
`watermark >= ws+size` el operador registra el aviso en `(ws+size)*TIME_SCALE - 1` y
comprueba `ws + size <= t + 1`; el `close_time` satura para no envolver la frontera
escalada ante event-times extremos.

## 9. Verificación

`crates/hotlap-engine/tests/window.rs` compara el changelog incremental con una
recomputación completa de primera mano (`recompute`) que agrupa los eventos por ventana,
descarta solo las inserciones tardías y aplica las retracciones; la cobertura de frontera
exacta, GC y retracciones vive en los tests de `hotlap-engine`
(`tests/engine_window.rs`).
