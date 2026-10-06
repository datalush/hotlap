# Hotlap — núcleo incremental: decisión y frontera

- Fecha: 2026-10-06
- Estado: decisión validada por spike (crate throwaway, fuera del repo)
- Alcance: núcleo de SP1 y forma de la frontera `IncrementalCore`
- Plan ejecutado: `~/.opencode/plan/2026-10-06-hotlap-core-spike.md`
- Spec: `~/.opencode/plan/2026-10-06-hotlap-streaming-engine-design.md` §7.8

## 1. Decisión

Se adopta **`differential-dataflow` 0.25.1** (+ **`timely` 0.31.0**) como núcleo
incremental, **detrás** del trait propio `IncrementalCore`. El núcleo no expone ni recibe
tipos de `differential-dataflow` ni de Arrow: el motor usa su propio modelo de cambio
(`RowChange` / Z-set), y Arrow 59 solo cruza los bordes (sources/sinks/SQL).

El spike confirma, en el árbol real:

- **Arrow-free:** `cargo tree -i arrow` y `cargo tree -i arrow-schema` no encuentran
  paquete alguno → el core no arrastra Arrow (nuestro 59 es el único Arrow del motor).
- **Licencia compatible:** ambos crates son **MIT** (repo Apache-2.0).
- **API válida:** el circuito `filter`-style (`map(key) → count`) con altas y
  **retracciones** produce el mismo resultado que la recomputación completa
  (differential testing), de forma repetible.
- **Frontera limpia:** el trait `IncrementalCore` compila sin que ningún tipo de
  `differential-dataflow` (ni de Arrow) aparezca en sus firmas.

**Resultado: DD valida como núcleo.**

## 2. Versiones y licencias (evidencia verbatim)

Salida de `notes/licenses.txt` (producida con `cargo metadata` + `jq`):

```
differential-dataflow 0.25.1 MIT
timely 0.31.0 MIT
```

Dependencias directas del spike: `differential-dataflow@0.25.1` y `timely@0.31.0`.
Rust del árbol: 1.97.1, edition 2024. Build en frío del crate (dev, sin optimizar):
**15.22 s** (ver `notes/build.txt`).

## 3. Ausencia de Arrow (evidencia verbatim)

```
$ cargo tree -i arrow
error: package ID specification `arrow` did not match any packages

$ cargo tree -i arrow-schema
error: package ID specification `arrow-schema` did not match any packages
```

`cargo tree -p differential-dataflow` (verbatim) confirma que el subárbol es
`columnar`/`columnation`/`timely*`/`serde`/`smallvec`/`bytemuck`/`bincode`/… — **sin Arrow**:

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

## 4. Circuito mínimo y differential test (retracciones)

Circuito: `SELECT key, COUNT(*) FROM input GROUP BY key` como
`rows.map(|(key, _value)| key).count()` sobre `differential-dataflow`, con eventos
empujados a tiempo lógico creciente y bajas como diffs negativos.

Fixture (intercala altas y retracciones, incluida una clave cuyo conteo cae a cero):

```
(1,10):+1  (1,20):+1  (2,30):+1  (1,10):-1  (3,30):+1  (2,30):-1  (1,20):+1
```

Resultado del oráculo de recomputación completa y del incremental: **`[(1,2),(3,1)]`**
(`key2` cae a 0 y se elimina). Salida verbatim de `cargo test`:

```
running 3 tests
test harness::oracle_is_well_formed ... ok
test boundary::boundary_matches_full_recompute ... ok
test dd::dd_matches_full_recompute ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

- `dd::dd_matches_full_recompute`: incremental ≡ recomputación completa.
- `boundary::boundary_matches_full_recompute`: mismo resultado **a través** del trait
  `IncrementalCore` (impl `DdCore`).
- Repetibilidad: el test corrió 3× y pasó en las tres (determinismo de la consolidación
  para el mismo changelog).

## 5. Tiempo lógico, no event-time

El "time" del circuito (los timestamps que avanzan con `advance_to`) es **lógico**: versiona
los Z-sets para que DD incrementalice y consolide por epoch/batch. **No** es event-time.
Event-time es un **atributo del registro** (p. ej. `ScanRecord.timestamp()` de Fluss) y se
trata con watermarks/ventanas como operador relacional normal (spec §7.6). Verificado en el
spike: los resultados son función del changelog, no del reloj wall-clock.

## 6. Frontera `IncrementalCore`

Forma validada (sin tipos de DD ni Arrow en las firmas):

```rust
/// Modelo propio del motor. No aparecen tipos de differential-dataflow ni de Arrow.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RowChange { pub key: i64, pub value: i64, pub diff: i64 }

pub trait IncrementalCore {
    /// Aplica un lote de cambios; devuelve el estado resultante (key, count).
    fn apply(&mut self, changes: &[RowChange]) -> Vec<(i64, i64)>;
}

/// impl v1: differential-dataflow (adaptador interno).
pub struct DdCore;
```

`DdCore` es un adaptador: convierte `RowChange` a su tipo interno y delega en el circuito DD.
El core es **intercambiable** (igual que `StateBackend`), lo que evita hipotecarse a un
núcleo concreto.

### Hallazgo de sesión con estado (Task 4 Step 5)

La implementación del spike es **stateless por llamada**: cada `apply` levanta un worker
`timely::execute_directly` nuevo, reconstruye el dataflow y **replaya todo el changelog**
(no aprovecha el estado retenido en los *arrangements* de DD). Para una sesión realmente
incremental, `DdCore` debe poseer:

1. un **worker timely vivo** (hilo propio que mantiene `worker.step()`), para que los
   operadores retengan sus arrangements entre pushes;
2. el **`InputSession`** para empujar nuevos lotes y `advance_to` con tiempo lógico
   **monótono**;
3. un **probe** sobre la salida, para saber cuándo el push se ha asentado antes de leer el
   Z-set de salida;
4. un **canal/handle** para que `apply` envíe el lote + avance de tiempo y espere la salida
   consolidada.

Implicación para SP1: `apply` debe recibir el **lote nuevo** (no el changelog completo) y
avanzar el tiempo lógico, dejando que DD actualice incrementalmente los agregados con
retracciones. (No se resuelve en el spike.)

## 7. Deriva de API observada (ergonomía)

El snippet del plan (API de versiones previas) no compiló tal cual; se ajustó a la 0.25.1.
Notas para SP1:

- El operador de conteo es el método inherente `Collection::count()` (devuelve
  `(K, R)`), no el trait `operators::Count`.
- `scope.new_collection()` devuelve `(InputSession, Collection)`; `insert`/`remove` existen
  solo para `R = isize` (`update` es el genérico).
- `Collection::probe()` devuelve `(Handle, Collection)` (el probe se toma de la **salida**,
  no del input).
- El bucle de drenaje debe avanzar el input hasta igualar el objetivo del probe
  (`probe.less_than(&input.time())`), no un time extra, o `worker.step()` no termina.

Esto confirma el aviso de la spec: DD es un **framework**, no un IVM automático; la
traducción plan→circuito (filtros, joins, aggregates, ventanas) es nuestra.

## 8. Riesgos y límites

- **Ergonomía "en desarrollo"**: la propia doc de DD lo advierte; el plan→circuito y los
  operadores son nuestros. Mitigado por la frontera `IncrementalCore` y por differential
  testing (incr ≡ recomputación completa).
- **Sin IVM automático**: no hay compilador SQL→circuito en el crate; hay que mapear el plan
  lógico de DataFusion a operadores DD y **rechazar explícito** lo no soportado.
- **Sesión con estado**: hoy el spike recomputa por llamada (ver §6); SP1 debe mantener el
  worker/estado vivo.
- **Coste de traducción de expresiones**: validar temprano con un caso tumbling.
- **Sin tercera opción embebible**: el único otro núcleo serio es `dbsp` (descartado por
  Arrow 58 + peso + compilador Java); Kaskada, ideal y **archivado**, es la advertencia.

## 9. Alternativas descartadas

- **`dbsp` (Feldera):** potente, pero arrastra **Arrow 58** (vía `feldera-types`) → segunda
  major de Arrow en el binario; deps pesadas para un motor embebido; y su IVM SQL
  "automática" no la aporta el crate (el compilador `sql-to-dbsp-compiler` es **Java**).
  Descartado.
- **Kaskada:** encaje ideal sobre el papel (embebido, Arrow nativo, incremental) pero
  **archivado** (`datastax-archive/kaskada`) → no es dependencia viable. Su caso motiva
  **no hipotecarse a un core concreto**, de ahí la frontera `IncrementalCore`.
- No hay una tercera opción embebible y mantenida.

## 10. Artefactos

- Crate throwaway del spike: `/tmp/opencode/hotlap-core-spike/` (no versionado; conservado
  para verificación independiente). Evidencia cruda en sus `notes/`.
- Este documento es lo único versionado del spike.
