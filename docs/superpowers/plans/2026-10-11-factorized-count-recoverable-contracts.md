# Contratos y fixtures del plan de COUNT(*) recuperable

Parte normativa del [plan de implementación](2026-10-11-factorized-count-recoverable.md). Las interfaces siguientes son propuestas que deben crearse en sus tareas, no firmas actualmente disponibles.

## Interfaces entre tareas

### Tarea 1: kernel dentro del módulo arrange

```rust
pub(crate) struct CountSpec {
    pub left_key: usize,
    pub right_key: usize,
    pub group_key: usize, // Index in the logical joined schema.
}
pub(crate) enum Eligibility { Schema, RowBound, NegativeState, NumericBound }
pub(crate) enum FactorizedError {
    Ineligible(Eligibility),
    Fatal(crate::EngineError),
}
// FactorizedCount owns both existing arrangements and wide key summaries.
impl FactorizedCount {
    pub(crate) fn new(left: SchemaRef, right: SchemaRef, spec: CountSpec)
        -> Result<Self, FactorizedError>;
    pub(crate) fn apply(&mut self, left: &ZSetBatch, right: &ZSetBatch)
        -> Result<ZSetBatch, FactorizedError>;
    pub(crate) fn output(&self) -> Result<ZSetBatch, crate::EngineError>;
}
```

`CountSpec` deriva Clone/Copy/Debug/PartialEq/Eq. Eligibility deriva Debug/Clone/Copy/PartialEq/Eq. FactorizedError deriva Debug; no requiere Clone de EngineError. Usar EngineError real en conversions fallibles. `output()` devuelve la imagen actual agregada, nunca un delta de reinicialización. Mover la serialización/digest y failpoints de pruebas detrás de cfg(test); no añadir flags de inyección al objeto de producción.

### Tareas 2 y 3: adapter canónico

```rust
impl FactorizedCount {
    pub(crate) fn export_logical(&self)
        -> Result<(GroupState, JoinState), crate::EngineError>;
    pub(crate) fn from_logical(spec: CountSpec, group: &GroupState, join: &JoinState)
        -> Result<Self, FactorizedError>;
}
pub(crate) fn validate_logical(spec: CountSpec, group: &GroupState, join: &JoinState)
    -> Result<(), FactorizedError>;
```

GroupState/JoinState proceden de `hotlap_core::snapshot`; no crear tipos persistidos. `from_logical` solo instala un kernel inicializado cuando las dos relaciones y el estado del agregado corresponden a inicialización completa elegible. Representaciones parciales válidas devuelven Ineligible(Schema) y quedan preservadas por el controlador legacy. Malformed IPC o coherencia falsa demostrable devuelve Fatal(EngineError::Infrastructure), no Ineligible.

### Tarea 4: controlador interno del grafo

```rust
struct LegacyCount { join: Join, group: GroupAggregate }
enum CountMode { Pending(LegacyCount), Factorized(FactorizedCount), Legacy(LegacyCount) }
pub(super) struct FusedCount { spec: CountSpec, mode: CountMode, applies: u64 }
impl FusedCount {
    pub(super) fn new(spec: CountSpec) -> Self;
    pub(super) fn apply(&mut self, left: &ZSetBatch, right: &ZSetBatch)
        -> Result<ZSetBatch, crate::EngineError>;
    pub(super) fn export_logical(&self) -> Result<(GroupState, JoinState), crate::EngineError>;
    pub(super) fn import_logical(&mut self, group: &GroupState, join: &JoinState)
        -> Result<(), crate::EngineError>;
}
impl LegacyCount {
    fn from_logical(spec: CountSpec, group: &GroupState, join: &JoinState)
        -> Result<Self, crate::EngineError>;
    fn apply(&mut self, left: &ZSetBatch, right: &ZSetBatch)
        -> Result<ZSetBatch, crate::EngineError>;
}
```

`LegacyCount::from_logical` crea Join::new con claves individuales y GroupAggregate::new con group_key + AggSpec::count(), y usa import_state existente. `LegacyCount::apply` pasa la salida de join.apply a group.apply. Pending contiene esos operadores vacíos; buffers pertenecen a Node::CountJoin, no al controlador. Import en representación parcial puede elegir Legacy; no es obligatorio promoverla después.

Algoritmo obligatorio: Pending prepara un kernel y aplica el primer par de batches sin instalarlo hasta éxito; si es Ineligible instala/aplica legacy. Factorized.apply con Ineligible exporta el estado ANTERIOR, prepara LegacyCount local y lo instala solo cuando la preparación termina; entonces aplica el mismo par de batches una vez. Fatal no activa fallback. Legacy jamás intenta volver a factorizar durante esa instancia. applies usa saturating_add solo tras éxito factorizado; no se persiste.

`ViewGraph::build(plan: &Plan)` conserva el default actual; añadir `build_with_factorized_count(plan: &Plan, enabled: bool) -> Self`, `factorized_applies(&self) -> u64` y un probe cfg(test) `factorized_modes(&self) -> Vec<&'static str>` con pending/factorized/legacy. Todos son internos. Registrar las pruebas del controlador como módulo hijo de factorized_count.rs para acceder a CountMode sin ampliar su visibilidad.

### Tareas 5 y 6: configuración y validación

```rust
impl EngineCore {
    pub fn set_factorized_join_count(&mut self, enabled: bool) -> Result<(), CoreError>;
}
impl SessionConfig {
    pub fn with_factorized_join_count(mut self, enabled: bool) -> Self;
}
impl SqlSession {
    pub fn with_factorized_join_count(mut self, enabled: bool) -> Self;
}
impl Checkpointer {
    pub(crate) fn with_factorized_join_count(mut self, enabled: bool) -> Self;
}
// New Pipeline field, initialized explicitly in every existing literal:
pub factorized_join_count: bool;
```

CoreError procede de hotlap_core. Los builders de sesión documentan uso anterior a START. Configuración privada del core y Checkpointer no se serializa. En restore, extender `rebuild_snapshot` con argumento bool y usarlo tanto en validación scratch como en restore real. En checkpoint_body.rs añadir `validate_engine_snapshot_with_factorized_count(snapshot: &EngineSnapshot, enabled: bool) -> Result<(), ConnectorError>`: configurar EngineCore nuevo y llamar validate_snapshot, conservando el mapeo actual de errores. El helper anterior delega con false; Checkpointer usa su flag. No añadir métodos a IncrementalCore/Hotlap: runtime configura el EngineCore concreto antes de boxearlo.

Añadir `ViewGraph::validate_restored_count_state(&self, plan: &Plan, output: Option<&ZSetBatch>) -> Result<(), EngineError>`. Recorrer plan y representación lógica juntos; detectar patrón también en grafo legacy. Validar cada par Group/Join elegible; solo comparar imagen de salida de vista cuando la raíz count está envuelta exclusivamente por Filter/Project stateless. Validar estado interno de grupos fusionados aunque exista otro operador exterior; no inventar una imagen de ventana a partir de count.

## Fixtures concretos para tests de engine

Crear helpers cfg(test) en `core/tests/factorized_count.rs`; reutilizarlos desde benchmark hijo. Para tests del módulo arrange, reutilizar batch/schema de soporte histórico y el `reference` real siguiente con su propio fixture de schemas.

```rust
use std::sync::Arc;
use arrow::{array::{ArrayRef, Int64Array}, datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch};
use hotlap_core::{AggSpec, IncrementalCore, InputId, Plan, ViewId, ZSetBatch};

pub(super) fn keys(rows: &[(Option<i64>, i64)]) -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
    let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(
        rows.iter().map(|r| r.0).collect::<Vec<_>>()))];
    ZSetBatch::new(RecordBatch::try_new(schema, columns).unwrap(),
        Arc::new(Int64Array::from(rows.iter().map(|r| r.1).collect::<Vec<_>>()))).unwrap()
}
pub(super) fn plan(group_key: usize) -> Plan {
    Plan::GroupAggregate {
        input: Box::new(Plan::Join { left: Box::new(Plan::Source(InputId(0))),
            right: Box::new(Plan::Source(InputId(1))), left_key: vec![0], right_key: vec![0] }),
        key: vec![group_key], aggs: vec![AggSpec::count()],
    }
}
fn reference(left: &ZSetBatch, right: &ZSetBatch, group_key: usize) -> ZSetBatch {
    let mut join = crate::ops::Join::new(&[0], &[0]);
    let mut group = crate::ops::GroupAggregate::new(&[group_key], vec![AggSpec::count()]);
    group.apply(&join.apply(left, right).unwrap()).unwrap()
}
pub(super) fn core(enabled: bool, group_key: usize) -> crate::EngineCore {
    let mut core = crate::EngineCore::new();
    core.set_factorized_join_count(enabled).unwrap();
    for id in [InputId(0), InputId(1)] { core.register_input(id).unwrap(); }
    core.build_view(ViewId(0), &plan(group_key)).unwrap();
    core
}
```

Definir comparación genérica de salidas en cada harness propietario con el código siguiente. El benchmark hermano importa helpers pub(super) desde factorized_count. La consolidación lógica de salidas no consolida inputs antes del guard. `assert_logical_eq` decodifica relaciones IPC con core::ipc::decode_zset y compara estos mapas, GroupState ordenado/schema decodificado, joined canónico vacío y buffers; no exige bytes IPC idénticos de mapas HashMap.

```rust
fn rows(z: &ZSetBatch) -> std::collections::BTreeMap<Vec<u8>, i128> {
    use arrow::row::{RowConverter, SortField};
    let converter = RowConverter::new(z.schema().fields().iter()
        .map(|f| SortField::new(f.data_type().clone())).collect()).unwrap();
    let encoded = converter.convert_columns(z.batch.columns()).unwrap();
    let diff = z.diff.as_any().downcast_ref::<Int64Array>().unwrap();
    let mut result = std::collections::BTreeMap::new();
    for index in 0..z.len() {
        *result.entry(encoded.row(index).as_ref().to_vec()).or_insert(0) += diff.value(index) as i128;
    }
    result.retain(|_, weight| *weight != 0);
    result
}
pub(super) fn assert_zset_eq(actual: &ZSetBatch, expected: &ZSetBatch) {
    assert_eq!(actual.schema(), expected.schema());
    assert_eq!(rows(actual), rows(expected));
}
#[test]
fn factorized_fallback_exactly_once() {
    let spec = CountSpec { left_key: 0, right_key: 0, group_key: 0 };
    let mut fused = FusedCount::new(spec);
    let mut legacy = LegacyCount {
        join: crate::ops::Join::new(&[0], &[0]),
        group: crate::ops::GroupAggregate::new(&[0], vec![AggSpec::count()]),
    };
    for (left, right) in [
        (keys(&[(Some(7), 2)]), keys(&[(Some(7), 3)])),
        (keys(&[(Some(99), -1)]), keys(&[])),
        (keys(&[(Some(99), 1)]), keys(&[])),
        (keys(&[(Some(7), -1)]), keys(&[])),
    ] {
        assert_zset_eq(&fused.apply(&left, &right).unwrap(),
            &legacy.apply(&left, &right).unwrap());
    }
    assert!(matches!(fused.mode, CountMode::Legacy(_)));
}
#[test]
fn factorized_kernel_weighted_key_only() {
    let left = keys(&[(Some(7), 2)]);
    let right = keys(&[(Some(7), 3)]);
    let spec = CountSpec { left_key: 0, right_key: 0, group_key: 0 };
    let mut kernel = FactorizedCount::new(left.schema(), right.schema(), spec).unwrap();
    assert_zset_eq(&kernel.apply(&left, &right).unwrap(), &reference(&left, &right, 0));
}
#[test]
fn factorized_import_rejects_inconsistent_groups() {
    let left = keys(&[(Some(7), 2)]);
    let right = keys(&[(Some(7), 3)]);
    let spec = CountSpec { left_key: 0, right_key: 0, group_key: 0 };
    let mut kernel = FactorizedCount::new(left.schema(), right.schema(), spec).unwrap();
    kernel.apply(&left, &right).unwrap();
    let (group, join) = kernel.export_logical().unwrap();
    let mut broken = group.clone();
    broken.groups[0].1.rows = 5;
    assert!(matches!(FactorizedCount::from_logical(spec, &broken, &join),
        Err(FactorizedError::Fatal(_))));
    let restored = FactorizedCount::from_logical(spec, &group, &join).unwrap();
    assert_zset_eq(&restored.output().unwrap(), &kernel.output().unwrap());
}
#[test]
fn factorized_core_restore_cross_modes() {
    for (origin, target) in [(false, true), (true, false), (true, true), (false, false)] {
        let mut running = core(origin, 0);
        running.tap_view(ViewId(0)).unwrap();
        running.push(InputId(0), &keys(&[(Some(7), 2)])).unwrap();
        running.push(InputId(1), &keys(&[(Some(7), 3)])).unwrap();
        let snapshot = running.checkpoint().unwrap();
        let mut restored = crate::EngineCore::new();
        restored.set_factorized_join_count(target).unwrap();
        restored.validate_snapshot(&snapshot).unwrap();
        restored.restore(&snapshot).unwrap();
        assert_zset_eq(&restored.snapshot(ViewId(0)).unwrap(),
            &running.snapshot(ViewId(0)).unwrap());
        for state in [&mut running, &mut restored] {
            state.push(InputId(0), &keys(&[(Some(7), -1)])).unwrap();
        }
        assert_zset_eq(&restored.take_changes(ViewId(0)).unwrap(),
            &running.take_changes(ViewId(0)).unwrap());
    }
}
```

Estas pruebas se reparten en sus módulos propietarios, con imports de FactorizedCount/CountSpec/FactorizedError desde arrange::factorized_join_count y GroupState/JoinState desde hotlap_core::snapshot. El código de fixtures es cfg(test), no una nueva API de producto.

### Tests propietarios y RED

Tarea 1, `factorized_kernel_weighted_key_only`: construir CountSpec(0,0,0), aplicar keys([(7,2)]) y keys([(7,3)]) y comparar schema/pesos con reference → (7,6,+1). Repetir group_key=1 con campo derecho renombrado `right_k`, ambos payloads y ambas orientaciones de solo clave. Capturar Fatal inyectado y digest idéntico; el test debe fallar si se devuelve Ineligible.

Tarea 2, `factorized_snapshot_matches_legacy`: mantener Join/Group reales entre frames; para cada frame aplicar ambos caminos y exportarlos. Frames por lado: semilla (7,2)/(7,3), incremento (7,4)/(7,2), retracción (7,-3)/vacío, NULL (NULL,1)/(NULL,1), retracción total (7,-3)/(7,-5). Counts esperados 6→30→15; NULL=1 y desaparición posterior de 7. Comparar salidas y estado después de CADA frame.

Tarea 3, `factorized_import_rejects_inconsistent_groups`: exportar fixture 2×3=6; clonar y mutar por separado rows a 5, Count a 5, values vacío, AggValue::SumInteger en vez de Count, group duplicado, group ausente, schema alterado y keybytes inválidos. Cada snapshot debe producir Fatal; la variante con estado negativo válido produce Ineligible y NO Fatal. Roundtrip de estado legítimo debe permanecer exacto.

Tarea 4, `factorized_graph_selection`: opción off, on, self-join, Count(col), dos agregados, filtro interior, Project exterior y group_key incorrecta. Verificar probe y salida; los casos no admitidos siguen legacy sin error nuevo.

Tarea 4, `factorized_pending_absolute_weights`: empujar izquierda [(7,1)] y después [(7,-1)] antes de conocer derecha. Confirmar dos buffers de filas originales en checkpoint y ninguna fila aplicada al arrangement; empujar derecha [(7,1)] y comprobar resultado vacío. Repetir cerca del límite por fila con fixture histórico de cancelación; verificar fallback según el guard conservador, no una falsa permanencia factorizada por consolidación prematura.

Tarea 4, `factorized_fallback_exactly_once`: semilla 2×3; introducir clave nueva unmatched con peso -1 para salir del dominio sin overflow. Comparar delta y estado con Join/Group persistentes, verificar legacy permanente aun tras retractar ese negativo y volver a datos elegibles. Inyectar fallo en preparación de legacy y afirmar digest previo, buffer y cero salida conservados. Fallo de codec/salida debe devolver error, no fallback. Para límites por fila/número usar fixtures de tests históricos y observar referencia en el mismo perfil, incluyendo catch_unwind independiente si la aritmética nativa hace panic.

Tarea 5, `factorized_core_restore_cross_modes`: extender la prueba anterior a checkpoint en cada etapa de la matriz siguiente; restore y validate_snapshot no cambian configuración ni métricas de validación. Continuar los mismos frames y comparar snapshot/output/take_changes contra core ininterrumpido. Tap habilitado antes de semilla; el gate pre-freeze no se elimina para permitir tap tardío después de restore.

| Etapa | Datos antes del checkpoint | Control posterior |
|---|---|---|
| Sin inicializar | Ningún push | Primer par produce 6 |
| Pendiente izquierda/derecha | Varios pushes de un lado | Otro lado aplica buffer una vez |
| Inicializado vacío | Push vacío de ambos schemas | Inserciones ponderadas y nombres exactos |
| No vacío/NULL | Semilla y claves NULL | Retracciones y actualización de ambos lados |
| Retraído a vacío | Retirar filas de ambos lados | Reinserción conserva schema |
| Fuera del dominio/fallback | Negativo unmatched válido | Ruta legacy admisible; reevaluación tras restart |

Tarea 6, SQL real: `SELECT l.k, COUNT(*) FROM l JOIN r ON l.k = r.k GROUP BY l.k`, dos fuentes Int64 con payloads. Reutilizar SourceFactory fake y registro de sinks de tests runtime existentes. Resultado 2×3=6, métrica de applies >0 con on y 0 con off; correr además group por r.k para fijar schema. Al reiniciar, fuentes registran offsets solicitados: deben ser los del checkpoint, nunca el origen del log.

Tarea 6, `factorized_recovery_preflight_has_no_effects`: crear checkpoint canónico válido y versiones corruptas semánticamente (no solo bytes IPC rotos); antes de arranque tomar logs de sink/backend/source. Corrupto no promueve, poda, abre escritor ni escribe; Storage/Unsupported se propagan como tales, conservando configuración. Si política existente permite descartar candidato corrupto, usar escenario sin candidato anterior recuperable para observar rechazo, y otro con candidato anterior para verificar selección correcta sin efectos durante inspección.

## Protocolo concreto de medición (tarea 7)

Cuatro workloads: distinct, duplicates, low, mixed, mismos fixtures/semillas y cardinalidades del harness del piloto. Cada proceso de integración elige modo por `FACTOR_COUNT_MODE=legacy|factorized`, caso por `FACTOR_COUNT_CASE`, repetición por `FACTOR_COUNT_REPETITION`. Test exacto `factorized_integration_measure`, ignorado por defecto. El builder de los fixtures se comparte con prueba no ignorada de corrección; no se copia un generator sin su gate.

Registrar `mod factorized_count_benchmarks;` en core/tests.rs. Comando por proceso: `CARGO_BUILD_JOBS=1 RUST_TEST_THREADS=1 FACTOR_COUNT_MODE=factorized FACTOR_COUNT_CASE=distinct FACTOR_COUNT_REPETITION=1 cargo test -p hotlap-engine --lib --release --locked core::tests::factorized_count_benchmarks::factorized_integration_measure -- --ignored --exact --nocapture`. Verificar el nombre con `--list` y comprobar un test ejecutado; un comando que ejecuta cero tests es un run inválido.

Inicializar dos fuentes, view y taps fuera de tiempos; 40 warm-up continuos y 400 pasos medidos. La métrica confirma ruta; salida analítica compacta confirma resultado final fuera del tiempo. Cronometrar push + drenaje/fold real; no incluir construcción de fixtures ni oracle. Capturar p50/p99 de pasos y tiempo total. Después medir checkpoint y restore en relojes separados; validar resultado, continuar un delta y medir transición por negativo unmatched en una clave nueva. Reportar en legacy el mismo delta como apply de comparación, NO como un fallback que no existe.

Por fila CSV registrar head, mode, case, repetition, seed, warmup, steps, measured_input_rows, total_apply_ns, p50_ns, p99_ns, checkpoint_ns, restore_ns, transition_apply_ns, checkpoint_bytes y VmHWM KiB cuando esté disponible. Throughput = measured_input_rows / total_apply_seconds; no contar filas retenidas como entradas nuevas. VmHWM incluye fixture, warm-up y fases posteriores: no atribuirlo exclusivamente al kernel. Guardar logs, comandos y exit inmediato; exigir un test ejecutado por proceso y cinco runs por modo/caso.

Mantener 40 procesos de integración separados de 60 procesos del harness de kernel. Calcular mediana/min/max de cada columna, pares por caso/repetition para ratios; conservar outliers. Si código cambia tras medición, repetir en nuevo HEAD las mediciones afectadas y registrar procedencia exacta; la entrega final usa únicamente matrices del HEAD final. No comparar números SQL/runtime con los del kernel como si midieran lo mismo.
> **SUPERADO (2026-10-11):** no implementar este contrato piloto COUNT. La dirección vigente es una arquitectura global del kernel (estado, batches, filtros, proyecciones, joins, agregados, ventanas y salidas), sin compatibilidad histórica; v1 es el único formato actual.
