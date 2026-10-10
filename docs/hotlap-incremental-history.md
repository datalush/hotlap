# Antecedentes históricos del núcleo incremental

Este documento conserva los experimentos y ejemplos técnicos previos al contrato
actual. No describe la arquitectura implementada. Ver
[contrato del núcleo incremental](hotlap-incremental-core.md).

## 2. Versiones y licencias históricas

Salida de `notes/licenses.txt` (producida con `cargo metadata` + `jq`):

```
differential-dataflow 0.25.1 MIT
timely 0.31.0 MIT
```

Dependencias directas del prototipo: `differential-dataflow@0.25.1` y `timely@0.31.0`.
Rust del árbol: 1.97.1, edición 2024. Build en frío del crate (dev, sin optimizar):
**15.22 s**.

> **MSRV verificado:** DD/timely declaran `rust_version = 1.86` y **compilan con 1.94.0** del
> repo (`rustc 1.94.0`, edición 2024; deps resueltas `differential-dataflow 0.25.1` /
> `timely 0.31.0`). Comprobado en un crate aparte con `rust-version = "1.94"`; sin errores de
> MSRV ni de edición.

## 3. Dependencias del experimento

```
$ cargo tree -i arrow
error: package ID specification `arrow` did not match any packages

$ cargo tree -i arrow-schema
error: package ID specification `arrow-schema` did not match any packages
```

`cargo tree -p differential-dataflow` (salida verbatim) confirma que el subárbol del
experimento dependía de `columnar`/`columnation`/`timely*`/`serde`/`smallvec`/
`bytemuck`/`bincode`/… y su frontera no incluía Arrow:

```
differential-dataflow v0.25.1
├── columnar v0.13.2
│   ├── bytemuck v1.25.2
│   ├── columnar_derive v0.13.2 (proc-macro)
│   └── smallvec v1.16.2
├── columnation v0.1.2
├── fnv v1.0.7
├── paste v1.0.15 (proc-macro)
├── serde v1.0.229
├── smallvec v1.16.2
└── timely v0.31.0
    ├── bincode v1.3.3
    ├── byteorder v1.5.0
    ├── columnar v0.13.2
    ├── columnation v0.1.2
    ├── getopts v0.2.24
    ├── itertools v0.14.0
    ├── serde v1.0.229
    ├── smallvec v1.16.2
    ├── timely_bytes v0.31.0
    ├── timely_communication v0.31.0
    ├── timely_container v0.31.0
    └── timely_logging v0.31.0
```

(Árbol completo en `notes/deps-dd.txt` del crate throwaway.)

## 4. Circuito de prueba histórico

Circuito: `SELECT key, COUNT(*) FROM input GROUP BY key`, expresado como
`rows.map(|(key, _value)| key).count()` en `differential-dataflow`, con eventos
empujados a tiempo lógico creciente y retracciones como diffs negativos.

Fixture (intercala altas y retracciones, incluida una clave cuyo conteo cae a cero):

```
(1,10):+1  (1,20):+1  (2,30):+1  (1,10):-1  (3,30):+1  (2,30):-1  (1,20):+1
```

Resultado del oráculo de recomputación completa y del cálculo incremental:
**`[(1,2),(3,1)]`**
(`key2` cae a 0 y se elimina). Salida verbatim de `cargo test`:

```
running 3 tests
test harness::oracle_is_well_formed ... ok
test boundary::boundary_matches_full_recompute ... ok
test dd::dd_matches_full_recompute ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

- `dd::dd_matches_full_recompute`: cálculo incremental ≡ recomputación completa.
- `boundary::boundary_matches_full_recompute`: mismo resultado **a través** del trait
  `IncrementalCore` (impl `DdCore`).
- Repetibilidad: el test corrió 3× y pasó en las tres (determinismo de la consolidación
  para el mismo changelog).

## 5. Tiempo lógico y tiempo de evento

El tiempo del circuito (marcas que avanzan con `advance_to`) es **lógico**: versiona
Z-sets para que DD incremente/consolide por epoch/lote. **No** es tiempo de evento,
que es un **atributo del registro** (p. ej. `ScanRecord.timestamp()` de Fluss) y se
trata con watermarks/ventanas como operador relacional normal. La prueba del
prototipo es determinista (mismo changelog ⇒ mismo resultado) y usa tiempo lógico,
no reloj de pared.

## 6. Frontera ilustrativa

Forma validada en el experimento (sin tipos DD ni Arrow en las firmas):

```rust
/// Engine-owned model. No differential-dataflow or Arrow types appear here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RowChange { pub key: i64, pub value: i64, pub diff: i64 }

pub trait IncrementalCore {
    /// Applies a batch of changes and returns the resulting (key, count) state.
    fn apply(&mut self, changes: &[RowChange]) -> Vec<(i64, i64)>;
}

/// v1 implementation: differential-dataflow (internal adapter).
pub struct DdCore;
```

`DdCore` adapta `RowChange` al tipo interno y delega en el circuito DD. El core es
**intercambiable** (como `StateBackend`), lo que evita acoplarse a uno concreto.

> El siguiente snippet es un esquema ilustrativo; no describe la API de producción.

### Estado entre lotes en el enfoque histórico

La implementación de producción conserva el estado de operadores entre lotes; no
reconstruye la vista completa en cada `apply`. El siguiente esquema describe
conceptos del enfoque anterior, no componentes requeridos en producción:

1. un **worker timely vivo** (hilo propio que mantiene `worker.step()`), para que los
   operadores retengan sus arrangements entre pushes;
2. el **`InputSession`** para empujar nuevos lotes y `advance_to` con tiempo lógico
   **monótono**;
3. un **probe** sobre la salida, para saber cuándo el push se ha asentado antes de leer el
   Z-set de salida;
4. un **canal/handle** para que `apply` envíe el lote + avance de tiempo y espere la salida
   consolidada.

`apply` recibe el **lote nuevo** (no el changelog completo) y aplica las altas y
retracciones al estado retenido.

## 7. Detalles de API del ejemplo histórico

Detalles de API ilustrativa del ejemplo:

- El operador de conteo es el método inherente `Collection::count()` (devuelve
  `(K, R)`), no el trait `operators::Count`.
- `scope.new_collection()` devuelve `(InputSession, Collection)`; `insert`/`remove`
  existen solo para `R = isize` (`update` es genérico).
- `Collection::probe()` devuelve `(Handle, Collection)` (el probe se obtiene de la
  **salida**, no de la entrada).
- El bucle debe avanzar entrada hasta igualar objetivo probe
  (`probe.less_than(&input.time())`), no un tiempo adicional, o `worker.step()` no termina.

El experimento mostró que el framework no generaba operadores SQL
automáticamente; traducción de filtros, joins, agregados y ventanas debía ser propia.

## 8. Lecciones y límites del enfoque previo

- **Ergonomía «en desarrollo»**: la documentación de la dependencia lo advertía;
  traducción y operadores son propios. Mitigado por `IncrementalCore` y pruebas
  diferenciales (incremental ≡ recomputación completa).
- **Sin IVM automático**: el crate no tenía compilador SQL→circuito; había que mapear
  plan lógico DataFusion a operadores y **rechazar explícitamente** lo no soportado.
- **Sesión con estado**: operadores producción retienen estado; recomputación
  completa solo sirve como oráculo de pruebas.
- **Coste de traducción de expresiones**: validar pronto con ventana tumbling.
- **Sin tercera opción embebible**: el otro núcleo serio era `dbsp` (descartado por
  Arrow 58, peso y compilador Java); Kaskada, ideal pero **archivado**, es advertencia.

## 9. Alternativas históricas

- **`dbsp` (Feldera):** potente, pero incorpora **Arrow 58** (vía `feldera-types`),
  segunda versión mayor de Arrow en el binario; dependencias pesadas para motor
  embebido; su IVM SQL «automática» no la aporta el crate (el compilador
  `sql-to-dbsp-compiler` está escrito en **Java**). Descartado.
- **Kaskada:** encaje ideal sobre el papel (embebido, Arrow nativo, incremental) pero
  **archivado** (`datastax-archive/kaskada`) → no es dependencia viable. Su caso motiva
  **no hipotecarse a un core concreto**, de ahí la frontera `IncrementalCore`.
- En la revisión de 2026-10 no se identificó tercera opción embebible mantenida.

## 10. Evidencia histórica

- El experimento aislado que originó las mediciones no forma parte del workspace.
- La implementación actual y sus pruebas en el workspace definen el contrato vigente;
  los resultados históricos no certifican su comportamiento actual.
