# COUNT(*) factorizado recuperable — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ejecutar el subconjunto aprobado mediante fusión opt-in, con checkpoints lógicos v5 y fallback a los operadores actuales.

**Architecture:** Separar el kernel del soporte experimental y añadir un adaptador lógico de estado. Un nodo físico conserva buffering, exporta Group + Join y cambia una sola vez a ejecución actual. Restore reconstruye resúmenes derivados y valida coherencia antes de cualquier promoción o efecto externo.

**Tech Stack:** Rust, Arrow, ZSetBatch, grafo incremental y codecs/checkpointer existentes; sin dependencias nuevas.

**Spec:** `docs/superpowers/specs/2026-10-11-factorized-count-recoverable-design.md`, aprobada conversacionalmente y después por escrito. Las [interfaces, fixtures y matrices](2026-10-11-factorized-count-recoverable-contracts.md) forman parte de este plan; no son otra fase independiente.

## Global Constraints

- Inner equijoin directo entre dos fuentes distintas.
- Una clave Int64 por lado; agrupación por esa clave y un único COUNT(*).
- Schemas Int64 fijos, incluyendo claves nullable y schemas de solo clave.
- Multiplicidades retenidas no negativas en la ruta optimizada; deltas signed.
- Conservación de la semántica actual de NULL, nombres, orden, tipos, nullability y pesos Z-set de las salidas.
- Sin filtros, proyecciones o agregados adicionales dentro del patrón fusionado. Una envoltura existente fuera del patrón no se elimina ni se reescribe.
- La opción está desactivada por defecto.
- El modo físico no se persiste.
- No se hacen merge, push, worktrees ni builds alternativos bajo /tmp.
- Los registros y planes externos anteriores se preservan.
- Sin nuevos tipos de snapshot, cambios de versión, SPI público nuevo, dependencias nuevas ni cambios de política de RowState.
- Comentarios de código en inglés; documentación de producto en español; sin `#[allow]`, DEBUG temporal ni etiquetas de seguimiento en código de producto.
- Un proceso Cargo a la vez; usar checkout y target existentes. Normal: `CARGO_BUILD_JOBS=1 RUST_TEST_THREADS=8`; medición: `CARGO_BUILD_JOBS=1 RUST_TEST_THREADS=1`. Nunca superar ocho jobs/threads.
- Rama ordinaria `feat/recoverable-factorized-count`, desde la rama del piloto, sin rebase/amend/stash/reset. No implementar hasta aprobar este plan; preservar el método acordado de subagentes y revisión por tarea.

## Review Focus

- Agrupar por la clave derecha con nombres/schema diferentes: conservar el nombre correcto y no asumir que la clave del agregado es la posición cero (tareas 1 y 4).
- Un lado pendiente recibe varios deltas cancelables antes de conocer el otro schema: preservar pesos absolutos originales y no confirmar buffers prematuramente (tarea 4).
- Checkpoint elegible pero corrupto con opción desactivada: rechazar coherencia falsa sin reinterpretarla como fallback ni reparar acumuladores (tareas 3 y 6).
- Restore válido fuera del dominio y reinicio después de fallback: admitir la ruta actual y decidir nuevamente al reiniciar, sin persistir el modo físico (tareas 3 y 5).
- Fallo operativo durante preparación/preflight: devolver su clase real y conservar estado/configuración, sin abrir escritores, promover o podar (tareas 4 y 6).

## Mapa de archivos y límites

Los nombres de archivos nuevos son decisiones del plan; las firmas del documento de contratos también. No son APIs existentes que puedan invocarse antes de crearlas.

| Unidad | Crear | Modificar |
|---|---|---|
| Kernel y dominio | `crates/hotlap-engine/src/arrange/factorized_join_count/kernel.rs`, `kernel_tests.rs` | `arrange.rs`, `arrange/factorized_join_count.rs`, `candidate.rs`, `domain.rs`, `support.rs` dentro del mismo módulo |
| Adaptador canónico | `crates/hotlap-engine/src/arrange/factorized_join_count/snapshot.rs`, `snapshot_tests.rs` | kernel y declaración de módulos |
| Control de ejecución | `crates/hotlap-engine/src/core/graph/factorized_count.rs`, `factorized_count_tests.rs` | `core/graph.rs`, `core/graph/apply.rs`, `core/graph/snapshot.rs` |
| Configuración y restore | `crates/hotlap-engine/src/core/tests/factorized_count.rs` | `core.rs`, `core/metrics.rs`, `core/traits.rs`, `core/restore.rs`, `core/tests.rs` |
| Opción runtime y SQL | `crates/hotlap-runtime/tests/sql_factorized_count.rs` | `session_config.rs`, `session/api.rs`, `session.rs`, `session/runtime.rs`, `runtime/pipeline.rs`, `runtime/engine.rs` dentro de hotlap-runtime |
| Preflight y recovery | `crates/hotlap-runtime/tests/factorized_count_recovery.rs` | `session/sources.rs`, `runtime/pipeline/preflight.rs`, `runtime/checkpoint.rs`, `runtime/checkpoint/pending.rs`, `runtime/checkpoint_body.rs` dentro de hotlap-runtime |
| Medición y guía | `crates/hotlap-engine/src/core/tests/factorized_count_benchmarks.rs`, `docs/performance/factorized-count-recoverable.md` | `core/tests.rs` |

Mantener los codecs de Join/Group y formatos de hotlap-core sin cambios. Reutilizar sus importadores para fallback. Si se necesita una operación de lectura interna del arrangement, añadir solo la operación específica, sin cambiar almacenamiento. Separar archivos por responsabilidad, con orientación de 400–500 líneas, no un límite artificial de 200.

### Tarea 1: kernel de producción con errores tipados

**Files:** unidad kernel del mapa. **Consumes:** Candidate existente y helpers checked del dominio. **Produces:** `CountSpec`, `Eligibility`, `FactorizedError`, `FactorizedCount::{new,apply,output}` del documento de contratos.

- [ ] Crear `kernel_tests.rs` y registrar el módulo cfg(test). Escribir la prueba ponderada del documento de contratos usando directamente FactorizedCount, no el adapter Candidate. Añadir clave derecha y fixture de schema de solo clave.
- [ ] Ejecutar `cargo test -p hotlap-engine --lib factorized_kernel -- --nocapture` con el entorno normal. Capturar primero el error por interfaz ausente; después, cuando compile, capturar RED conductual de error fatal clasificado como inelegibilidad mediante inyección cfg(test). Guardar comando y exit inmediatamente en `/tmp/opencode/factorized-count-recoverable/task-1/`.
- [ ] Extraer staging/commit/build_output a `kernel.rs`, todavía dentro de arrange para conservar acceso a helpers canónicos. El módulo padre deja de estar entero bajo cfg(test); Candidate, Reference, oracle, manual y benchmarks permanecen cfg(test). Sustituir dependencia de `support::Key/Output/empty_batch` por tipos locales y `ZSetBatch::empty`. Exportar dentro del crate los contratos del kernel desde `arrange::factorized_join_count`; no ampliar exports públicos de la librería.
- [ ] Implementar las firmas y clasificación de errores indicadas en contratos. Añadir `group_key` a la derivación real de schema mediante Join + Group sobre batches vacíos; comprobar que representa la clave izquierda o derecha. No adivinar nombres/nullability ni emitir columnas según nombres de campos.
- [ ] Conservar Candidate como adapter cfg(test) hacia el kernel, manteniendo digest, fallos inyectados y pruebas históricas. Los adapters pueden mapear errores para el harness antiguo; la API de producción nunca convierte errores de codec/salida en NotEligible.
- [ ] Ejecutar el filtro, los 800 frames existentes y toda la suite `cargo test -p hotlap-engine --lib`. Comprobar rollback de arrangements, resúmenes, salida y contadores ante cada error previo al commit.
- [ ] Revisar diff y commit lógico `refactor(engine): extract factorized count kernel`; si extracción y clasificación superan ~300 líneas, separar commits pequeños verificados, sin commit intermedio de código que no compila. Revisión independiente de tarea antes de avanzar.

### Tarea 2: exportación canónica sin enumerar pares

**Files:** `snapshot.rs`, `snapshot_tests.rs` del módulo factorizado, kernel y módulos. **Consumes:** CountSpec y kernel de tarea 1. **Produces:** `FactorizedCount::export_logical` del contrato.

- [ ] Escribir prueba `factorized_snapshot_matches_legacy` de contratos. Cubrir inicialización vacía, grupos no vacíos, ausencia tras retracción, NULL y schema de agrupación derecha. Comparar Group.rows, AggValue::Count, keys y schema, además de ambas relaciones del Join.
- [ ] Ejecutar `cargo test -p hotlap-engine --lib factorized_snapshot -- --nocapture` y capturar RED. Un error por método ausente permite arrancar la tarea, pero no sustituye el RED de representación que se obtiene al conectar el exportador mínimo.
- [ ] Implementar `export_logical`: exportar arrangements con el codec existente, `joined: Vec::new()`, pendientes None (pertenecen al nodo), groups ordenados con rows=count y un único AggValue::Count. Guardar schema completo de entrada del agregado derivado del join, nunca el schema físico reducido.
- [ ] Exportar grupos usando el converter lógico para la clave seleccionada. Conservar inicializado vacío distinto de sin inicializar: el nodo pendiente usa operadores actuales vacíos; un kernel instalado solo existe tras apply exitoso.
- [ ] Añadir control de trabajo cfg(test): relaciones de 10.000 filas izquierdas y 10.000 derechas bajo una clave, exportar sin construir 100 millones de pares. Verificar contadores de lectura de filas/grupos y ausencia de llamadas a Join.apply con datos retenidos.
- [ ] Ejecutar filtro y engine completo, revisar diff y commit `feat(engine): export canonical fused count state`. Revisión independiente antes de avanzar.

### Tarea 3: importación, elegibilidad y validación lógica

**Files:** snapshot y sus tests. **Consumes:** export_logical. **Produces:** `validate_logical` y `from_logical` del contrato; clase de corrupción separada de Eligibility.

- [ ] Escribir bucle de corrupción de contratos y matriz de restore; ejecutar `cargo test -p hotlap-engine --lib factorized_import -- --nocapture` y capturar RED auténtico de inconsistencia aceptada.
- [ ] Implementar reconstrucción desde relaciones decodificadas: sumar cada fila una vez en u128 checked, excluir buffers aún pendientes y comprobar no negatividad y productos Int64. No llamar al kernel apply con todo el estado ni enumerar pares: sus límites para deltas originales no son el criterio de elegibilidad de restore.
- [ ] Decodificar claves y grupos antes de insertarlos en mapas; rechazar keys duplicadas/inválidas, longitud o variante incorrecta de values, rows/Count distintos y grupos extra/ausentes dentro del dominio. Comparar schema lógico exacto y producto esperado por clave.
- [ ] Devolver Eligibility para estado válido no optimizable; devolver EngineError para IPC dañado o contradicción demostrable. No arreglar groups y no imponer ecuaciones de conteos no negativos a estados legacy con pesos negativos. Para representación histórica `joined`, seguir la política existente de Join.import_state: no usarla como nueva fuente de verdad.
- [ ] Probar lados sin inicializar/parciales, buffers, ambos schemas inicializados vacíos, conteos unmatched superiores a i64::MAX (varias filas válidas), pesos negativos válidos y productos fuera del dominio. Conservar representación parcial en modo actual, no sintetizar un kernel inicializado.
- [ ] Ejecutar filtro y engine completo, revisar diff y commit `feat(engine): validate and restore fused count state`. Revisión independiente antes de avanzar.

### Tarea 4: selección del grafo y fallback permanente

**Files:** unidad control del mapa; `core/metrics.rs` y `core.rs` solo para publicar observación de ruta exitosa. **Consumes:** kernel y adapter canónico. **Produces:** `FusedCount`, `LegacyCount`, nuevo Node::CountJoin y `ViewGraph::build_with_factorized_count` del contrato.

- [ ] Escribir `factorized_graph_selection`, `factorized_pending_absolute_weights` y `factorized_fallback_exactly_once` de contratos. Ejecutar `cargo test -p hotlap-engine --lib factorized_graph -- --nocapture` y filtros pending/fallback para capturar sus RED antes de cada implementación correspondiente.
- [ ] El compiler reconoce GroupAggregate con key de longitud uno, aggs exactamente `[AggSpec::count()]` y Join con claves individuales y dos Source distintos. Conserva Filter/Project exteriores, excluye transformaciones interiores y self-join. La elección final de group_key y schemas ocurre al conocer ambos lados.
- [ ] Añadir Node::CountJoin con hijos Source, controlador y dos buffers. Reutilizar accumulate/source_eval sin consolidar deltas antes de la comprobación de valores absolutos. Preparar acumulación antes de sustituir buffers; aplicar ambos solo cuando están disponibles; limpiar buffers solo tras éxito.
- [ ] Implementar flujo de modo del contrato. La preparación fallible de LegacyCount importa el par Group/Join exportado ANTES del delta; solo después instalar modo actual y aplicar ese delta una vez. Un Fatal conserva modo/estado previo; tras errores de apply legacy rige el fallo del core existente.
- [ ] Export/import del nuevo nodo consume exactamente Group, Join y ambos subárboles Source; preserva None stateless y buffers. Mantener cero contribución a contadores de ventanas. No persistir modo ni resúmenes.
- [ ] Registrar `factorized_join_count_applies` en métricas existentes, incrementándolo solo por apply factorizado exitoso, también vacío. Leer diferencia del contador interno del grafo antes/después de push; contador físico arranca en cero tras restore. No añadir métodos al trait IncrementalCore ni un diagnóstico público especial para tests.
- [ ] Pruebas: negativos, guard numérico conservador con neto cancelado, guard por fila que falla sin neto negativo, schema válido no Int64, fallo de preparación inyectado, error fatal de salida, estado posterior permanentemente legacy. Comparar contra referencia en el mismo perfil; no asumir panic debug en release.
- [ ] Ejecutar engine completo debug/release y strict clippy. Dividir selección, fallback y estado del nodo en commits verificados de una responsabilidad (~300 líneas), con mensajes `feat(engine): ...`. Revisión independiente de la tarea completa.

### Tarea 5: opción del core y restore cruzado

**Files:** unidad configuración/restore del mapa; helper de validación en graph/snapshot.rs. **Consumes:** build_with_factorized_count y validation del adapter. **Produces:** `EngineCore::set_factorized_join_count` y reconstrucción coherente configurada.

- [ ] Escribir `factorized_core_restore_cross_modes` de contratos y ejecutar `cargo test -p hotlap-engine --lib factorized_core -- --nocapture`; capturar RED de opción perdida durante scratch rebuild.
- [ ] Añadir bool privado false en with_registry. Setter exige core healthy, sin views y no frozen; devolver CoreError::Unsupported si se intenta cambiar después. build_view usa el flag, incluidos late views permitidos por retention; no reconstruir views existentes.
- [ ] Pasar el flag explícitamente a rebuild_snapshot tanto desde restore como validate_snapshot. Mantener aislamiento de métricas y swap del core solo después de reconstrucción completa; no alterar invalidación de retention.
- [ ] Validar coherencia del patrón count directo en el grafo lógico restaurado, también con opción apagada si sus relaciones son elegibles. Validar salida retenida para raíz count y envolturas stateless aplicando Filter/Project sobre la salida agregada reconstruida, no sobre pares. Pending changelog es historia aún no drenada: validar su codec/schema, NO exigir que sea igual a la salida actual.
- [ ] Probar matriz completa del documento de contratos en ambos sentidos; luego continuar inserciones/retracciones y comparar cambios y schemas. Comprobar grupos válidos negativos no elegibles en modo actual, fallback→checkpoint→restore con reevaluación de modo. Conservar gate existente de taps: un snapshot congelado rechaza habilitación tardía; uno no congelado permite tap pre-push sin backfill.
- [ ] Ejecutar debug/release engine y clippy; commits separados para plumbing y validación de salida si excede ~300 líneas. Revisión independiente antes de avanzar.

### Tarea 6: runtime opt-in, SQL real y preflight sin efectos

**Files:** unidades runtime/SQL/preflight del mapa; pruebas existentes de Pipeline literals. **Consumes:** setter del core, validate_snapshot configurado y métrica de ruta. **Produces:** setters SessionConfig/SqlSession y flag Pipeline del contrato.

- [ ] Escribir la prueba SQL del contrato y pruebas runtime de checkpoint corrupto. Capturar primero RED de ruta no seleccionada y después RED de escritor/efecto iniciado antes de detectar corrupción. No confundir error de interfaz ausente con esa evidencia conductual.
- [ ] Añadir flag false a SessionConfig, SqlSession y Pipeline; setters del contrato. Extender into_parts y Session::open. Pasar valor en session/runtime.rs y configurar EngineCore antes de Hotlap::open_with en runtime/engine.rs. Actualizar cada literal Pipeline existente con false; no cambiar IncrementalCore ni añadir setter a Hotlap.
- [ ] Mantener la validación semántica YA presente antes de escritores: `runtime/checkpoint/pending.rs` llama `validate_engine_snapshot` tanto al leer candidatos publicados como cuerpos pending. Añadir contexto físico efímero bool a Checkpointer y el builder interno del contrato; propagarlo desde session/sources.rs, pipeline/preflight.rs y runtime/engine.rs antes de selección/recovery. En checkpoint_body.rs añadir validator configurado y conservar helper default false para callers anteriores.
- [ ] No cambiar el significado del último argumento de `validate_sources_and_views`: es `Option<&[SinkDescription]>`, no un core de validación. Mantener selección en views.rs y mapeos Corruption/Missing versus Storage/Unsupported. Validar también al recuperar el candidato seleccionado; no confiar en que el backend no cambió desde preflight. Si se descarta un candidato conforme a política vigente, no promover/podar ni abrir escritores por haberlo inspeccionado.
- [ ] La prueba usa dos fuentes resumables y sink con registros de open/write/prepare/commit; también backend con registro de put/delete/promotion. Cubrir corrupción de Count/rows/schema/output, fallo Storage y versión Unsupported; afirmar sin efectos previos a aceptación del candidato. Un snapshot válido no elegible debe arrancar por legacy.
- [ ] SQL: definir la consulta real, comprobar plan aceptado sin reescribir traductor, emitir ambas fuentes y verificar métrica >0 solo con opción habilitada más schema y pesos de resultado. Si la consulta produce un patrón no cubierto, fijar el fixture SQL para la forma soportada; no ampliar la optimización silenciosamente para pasar la prueba.
- [ ] Ejecutar `cargo test -p hotlap-runtime --test sql_factorized_count` y `--test factorized_count_recovery`, después suite runtime completa. Commits separados de wiring, selección SQL y preflight por responsabilidad, todos GREEN; revisión independiente de conjunto.

### Tarea 7: medición de integración y documentación

**Files:** unidad medición/guía del mapa. **Consumes:** todas las tareas funcionales. **Produces:** mediciones reproducibles, no nuevos objetivos de velocidad.

- [ ] Crear prueba no ignorada `factorized_integration_workloads_match` para las cuatro cargas del piloto a través de EngineCore y fixture SQL representativo. Comparar cada salida, mapa acumulado y export lógico; asegurar selección mediante métrica. Ejecutar RED antes de implementar el builder de carga; no usar una aserción invertida como sustituto.
- [ ] Crear el benchmark ignorado `factorized_integration_measure` siguiendo protocolo de contratos. Builders/preflight de corrección quedan fuera del cronómetro; no introducir oracle en cada paso medido. Las fases apply/fold, checkpoint, restore y fallback tienen relojes separados.
- [ ] Ejecutar la prueba de medición reducida con los dos modos y comprobar resultados analíticos antes del run completo. Documentar en español configuración, domain/fallback, recovery cruzado, coste de snapshot y límites; no publicar cifras del piloto como cifras de integración.
- [ ] Commit código/protocolo GREEN antes de medir; obtener HEAD inmutable. Ejecutar 40 procesos de integración (2 modos × 4 cargas × 5 repeticiones), un Cargo/proceso a la vez. Repetir los 60 procesos del kernel original usando su harness existente sobre ese mismo HEAD; mantener matrices externas anteriores sin borrarlas.
- [ ] Generar CSV, exit-manifest y resumen min/mediana/max con Python stdlib desde todos los runs; ratios p50 emparejados por repetición. Guardar fuente HEAD, comandos/env, semillas, warm-up y logs en `/tmp/opencode/factorized-count-recoverable/final/`. Nunca excluir outliers ni promediar percentiles para presentar una mediana.
- [ ] Revisión independiente de mediciones/protocolo y documentación; no exigir mejora de RSS o un factor fijo de latencia para declarar corrección.

### Tarea 8: verificación y revisión de toda la rama

**Files:** sin nuevos archivos de producto; registros externos. **Consumes:** entregables 1–7. **Produces:** evidencia de entrega y seguimiento truthful.

- [ ] Ejecutar secuencialmente con entorno normal: `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo test -p hotlap-engine --lib --locked`, `cargo test -p hotlap-engine --lib --release --locked`, `cargo test --workspace --locked --no-fail-fast`.
- [ ] Registrar cada exit y recuentos reales; los tests ignorados no son benchmarks ejecutados. Verificar 40/40 integración y 60/60 kernel solo si sus manifests lo demuestran en HEAD final.
- [ ] Obtener revisión independiente completa base→HEAD: equivalencia lógica, dominio, fallback, buffering, v5, restore/validate configurados y ausencia de efectos tempranos. Resolver hallazgos con RED y commits pequeños; repetir mediciones si cambia código medido, no reutilizar resultados de otro HEAD.
- [ ] Confirmar git status, ausencia de archivos locales/generados añadidos y que IncrementalCore/formato/dependencias no cambiaron. No hacer merge ni push.
- [ ] Cerrar únicamente trabajo verificado en kata con SHA/evidencia; dejar necesidades humanas señalizadas si queda algo abierto. Publicar resultado kernel versus integración, costes de recovery/fallback y límites, sin certificaciones nuevas.

## Cobertura y handoff

La especificación 1–4 corresponde a tareas 1/4/5/6; 5–6 a tarea 4; 7–10 a tareas 2/3/5/6; 11 a tarea 8; 12 a tarea 7. Los cinco focos de revisión tienen tests en sus tareas propietarias y fixtures concretos en el documento de contratos.

Antes de ejecutar, leer ambos documentos y conservar todos los registros del piloto. Crear subissues kata por tarea después de aprobar el plan, buscando primero para evitar duplicados. Cada implementador recibe spec, plan, contratos, ref propia y APIs ya verificadas de sus dependencias; cada revisor recibe diff completo de su tarea. No repartir ediciones del grafo o Cargo entre agentes simultáneos.

**Pendiente:** revisión humana de este plan. El método de ejecución preservado es subagentes con revisión por tarea y revisión final; no se inicia ninguna tarea funcional durante la redacción del plan.

La investigación de memoria queda fuera de esta entrega: compactación exacta, arrangements compartidos y estado de solo resúmenes son alternativas de una fase independiente. No se descartan payloads en esta integración: son necesarios para sus controles por fila y para reconstruir el fallback canónico. Una futura investigación debe separar memoria viva del operador, fixtures, buffers temporales y RSS/checkpoint, y declarar explícitamente qué garantías conserva.
