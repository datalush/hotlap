# Historial técnico de la auditoría Rust

Registro de decisiones y resoluciones posteriores a la auditoría inicial. Conserva
los hechos técnicos y la procedencia histórica; no constituye una lista de tareas.

## 1. Código transitorio histórico

| Categoría histórica | Pieza | Disposición técnica |
| --- | --- | --- |
| Planificación | Sustituciones de planner default en DELETE/MERGE motivadas por FFI | Sustituir/justificar en composición nativa; no revertir a ciegas |
| Planificación | Coalesce explícito de sink motivado por FFI | Retirar con enforcement nativo verificado o conservar por necesidad Rust independiente |
| Binding | `FlussPlanner` / `FlussExtension` requeridos para DML | Preferir FFI oficial de tablas; retirar al cubrir contratos aceptados |
| Planes opacos | `OpaqueQueryPlanner`, registro/tokens/codecs opacos | Retirar transporte sustituido; no es mecanismo de planes duradero/distribuido |
| Recursos | `session_with_runtime`, `ResourceProvider`, `RuntimePlan` en crate FFI genérico | Sustituir por contrato de recursos; Rust no debe depender de reconstruir sesiones |
| Recursos | Cápsula/adaptador genérico de memoria | Candidato a contrato upstream, no borrado automático; preservar identidad consumidor/reserva y recursos |
| Host | Patch, build script, markers/restricciones de versión y exports auxiliares | Retirar lo sustituido por soporte oficial; conservar delta solo con justificación explícita |
| Escritura | Ruta append actual por fila | Sustituir donde ruta batch segura la supera; conservar API fila para uso real, no por fallback duplicado |
| MERGE | Adaptador de fuente física a provider `merge.rs::InputPlan` | No obsoleto automáticamente: recibe fuente física y compone operadores lógicos DF; conservar salvo mecanismo nativo más simple y semánticamente equivalente |
| Documentación | Descripciones históricas/API antigua (incluida introducción solo finita) | Actualizar docs canónicas; preservar historia verificada fuera de ramas ejecutables de compatibilidad |

El wrapper de métricas, validación de particiones/offsets, ACK del writer,
conversión de esquema y pruning conservador **no** son código sobrante por ser
específicos del conector. Funciones pequeñas repetidas de mapeo de errores no
justifican por sí solas un framework genérico. Dividir `write.rs` por responsabilidades
coherentes solo tras definir contratos públicos, no en capas que reenvían cada llamada.

## 2. Mapa histórico de evidencia y verificaciones posteriores

| Evidencia y proveniencia | Alcance observado | Comprobaciones dirigidas pendientes en ese momento |
| --- | --- | --- |
| SQL log nativo activo; `tests/live_log_sql.rs` | Proyección, pruning vs filtros exactos, evolución esquema, snapshots, reejecución/concurrencia y tablas recreadas | Planner/ruta nativa propia; vida de lotes retenidos; proyección tras limpieza planner |
| Retención remota; `tests/remote_retention.rs`; commit `7306be7` | Perfiles históricos RAM/disco/concurrencia y pressure | Colas poll decodificadas, admisión previa a decode, lotes retenidos y ruta batch modificada |
| S3/STS real; `tests/remote_retention.rs`; commit `7306be7` | Errores S3 reales, retención, expiración/renovación STS | Repetir solo rutas cambiadas; no repetir expiración 900 s para auditoría estática |
| Fuentes streaming; `tests/live_log_sql.rs`; commit `89e111b` | Appends tardíos/idle/cancelación, fallos topología y fuentes streaming | Progreso inicial vacío/podado, solapamiento mismo contexto, colas Arrow pendientes |
| SQL escritura; `tests/write_sql.rs`; commits `f66e90c`, `d06288a` | INSERT log/KV, DELETE, entrada 4 MiB con writer 2 MiB, escrituras concurrentes/continuas y layouts mixtos | Append batch con particiones/conteos distintos, cancelación ACK/enqueue, plazo preparación, resultado parcial |
| SQL MERGE; `tests/write_sql.rs`; commit `d06288a` | Acciones ordenadas, lógica NULL, duplicados, PK compuesta/rescale | Scratch payload ancho no-clave, duplicados tardíos/conocimiento parcial, writers/fuentes propios |
| Pruebas cliente `record/arrow.rs`, `accumulator.rs`, scanners | Normalización esquema/NULL, cierre limiter y límites reader/scanner | Identidad buffers preconstruidos, layout efectivo, bytes retenidos vs permisos |
| Pruebas FFI históricas | Casos reales de buffers/vida entre bibliotecas y rechazo pool | No prueban decode/routing nativo ni paridad runtime |

Muchas integraciones se ignoran por defecto y requieren fixtures Fluss aislados o
Docker/RustFS. Leer su código/evidencia histórica no equivale a ejecución reciente.
Las nuevas pruebas debían cubrir contratos de integración, no duplicar SQL/operadores
DataFusion ni imponer una estructura de refactorización.

## 3. Entrega y aceptación de la auditoría

Contratos Rust públicos, planificación/recursos nativos e integración completa
se analizaron por separado. Los hallazgos técnicos y las resoluciones posteriores
se conservan con sus límites en [detalle del historial](rust-implementation-history-details.md).

Esta auditoría registra inventario, hallazgos basados en código, matriz de
responsabilidades/disposiciones y mapa de evidencia/brechas. Los riesgos marcados
«por verificar» siguen pendientes; no se presentan como bugs reproducidos. Ningún
hallazgo autoriza borrar cambios desconocidos del árbol ni crear un framework SQL/runtime.
