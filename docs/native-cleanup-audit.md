# Inventario de fuente nativa y limpieza

Este inventario conserva la historia de fuentes, licencias y evidencia de la
implementación nativa.

Registra ciclo de aceptación nativa y disposición actual de fuentes en
[estructura Hotlap](hotlap-layout.md): los bindings importados y árbol Java/referencia
antes excluidos se retiraron del árbol de trabajo; historial/licencias permanecen en Git.

Auditoría del árbol de trabajo, 2026-10-05. Se validaron providers DataFusion nativos
e integraciones de motor; se retiró infraestructura sustituida para preparar entrega
nativa. Ninguna etapa implementa ni acepta bindings Python/FFI.

Cambios y mediciones describen snapshots registrados; verificación Git limpia final
se documenta por separado.

## Inventario resuelto

| Componente | Disposición y evidencia |
| --- | --- |
| Crate activa Python/FFI, extensiones/adaptadores host, scripts build, ejemplos/pruebas Python y manifests raíz Python | Retirados; workspace raíz contiene crates Rust nativas. Sources activos del conector ya no tienen referencias fallback FlussPlanner/OpaqueQueryPlanner/ResourceProvider/RuntimePlan/session_with_runtime/PyCapsule. |
| Fuentes upstream importadas Python/C++/Elixir | Conservadas como historial/referencia importada y excluidas explícitamente del workspace cliente. Sin dependencia/gate activo; avisos/licencias importados se conservan. |
| Cliente nativo | Implementación única necesaria de protocolo/auth, metadatos, routing bucket, codecs, scanners, buffers/colas writer, ACK/reintentos. Ejecución raíz y validación cliente independiente usan locks fijados propios. |
| Providers/adaptadores ejecución DataFusion y catálogo | Integración necesaria `TableProvider`/`ExecutionPlan`, contratos identidad/offset/snapshot y catálogo metadatos opcional; ejecutan sesiones/planners/runtime reales del llamador. Sin motor/transporte/scheduler alternativo. |
| Extensión MERGE nativa | Adaptación necesaria a API DataFusion fijada. Grafo auxiliar usa planner llamador y operadores estándar join/filter/CASE, no otra implementación SQL/runtime. Pruebas nativas UDF/planner y acción/NULL/datos la ejercitan. |
| `resources.rs`, `WriterMemoryAccounting` nativo y leases owners Arrow | Adaptadores de admisión/propiedad sobre `MemoryPool` real del llamador y owners cliente nativos. Pruebas vida buffer/worker/frame y perfiles pressure/cancel justifican mantenerlos; no son allocator global ni límite RSS. |
| Offsets, descubrimiento particiones y resúmenes progreso/terminal | Necesarios para identidad ejecución finita/continua y conocimiento conservador de aplicación. Pruebas reejecución/solapamiento/invalidación/lag/ACK parcial los sustentan; no son checkpoints persistentes/replay. |
| Errores/opciones tipados y métricas labels fijos | Necesarios para límites por operación y propagación error original. `ScanKv` usa ahora camino métricas RPC existente, corrigiendo bytes lectura KV sin reportar. Sin jerarquía retry alternativa. |
| Override core DataFusion vendorizado | Protección genérica necesaria de DELETE/UPDATE vacío y restricciones no soportadas para 55.1/Arrow59. Proveniencia/checksum/condición retiro en `vendor/README.md`; lo verifican tres pruebas planner genéricas y DELETE nativo real. |
| Feed/collector/histogramas exclusivos perfiles | Entrada `StreamingTable` controlada y medición acotada con interfaces estándar. Solo viven en tests/support; no añaden pool/allocator/decoder/transporte producción y sustituyen mediciones debug-recorder reiniciadas del perfil writer. |
| Ejemplo query nativo | Usa `SessionContext`/`RuntimeEnv`/providers estándar, configuración Fluss y motor separada, y emite por lote. Aceptación no necesita aplicación/scheduler externo. |

Hallazgos FFI históricos siguen identificados en `rust-implementation-audit.md`.
Instalaciones de laboratorio, wheels y credenciales externas no son artefactos de
entrega y no se retiraron/modificaron como atajo de limpieza.

## Grafo de dependencias y evidencia de fuente aislada

Export de fuentes visibles para Git (archivos rastreados conservados más fuentes/
tests/docs sin rastrear, excluyendo caches/secrets ignorados y `.git`) contenía 4.281
archivos, SHA256 `a376f2ab58d320b580c4f7e97390236aa0eb67a151b448d5b5f608fb6f59273c`.
Se copió a `/tmp/opencode/native-source-a773908601384a5fb6cb43d308dde670`. Un
`CARGO_TARGET_DIR` separado inicialmente vacío reconstruyó el grafo nativo **offline
con lock**, usando dependencias registry ya descargadas, en 4m30s.

- Grafo resuelto raíz: 453 paquetes; workspace `fluss-datafusion`; DataFusion 55.1.0,
  Arrow 59.3.0, cliente Fluss Rust 1.0.0.
- Grafo cliente: 431 paquetes; workspaces `fluss-rs`, `gen`, `fluss-test-cluster`,
  `fluss-examples`; lock independiente fija Arrow 59.0.0. Como dependencia raíz usa
  Arrow 59.3.0 del lock raíz. Ambos son Arrow59; esto no afirma ABI/paridad bindings.
- Ningún grafo resuelto activo contiene PyO3, arrow-pyarrow, datafusion-ffi,
  datafusion-ffi-ext, datafusion-python-util, fluss-datafusion-python, Stabby o
  Rustler. Manifests opcionales/importados no se confunden con nodos resueltos activos.
- Desde fuente/target aislados pasaron **29 core + 3 planner**, **8 SQL escritura real
  (41,57 s)** y **9 SQL lectura live real (78,83 s)**. Credenciales/CA/kubeconfig son
  entradas externas de laboratorio, no se copiaron al export.

Esto prueba build/ejecución aislados de ese snapshot nativo del árbol de trabajo.
Opción allocation-control y ejemplo nativo se añadieron después. Snapshot final de
4.284 archivos en `/tmp/opencode/native-source-7027470b6d924c44999d3e2c27d8140b`
tiene SHA256 `e1033ccaf11a6211ffab63206bbead9fd04e9cedb190f816bf4001d100222c80`.
Código nativo sin cambios reutilizó cache target aislado; pasaron sus pruebas core/
planner 29+3. El ejemplo se compiló offline desde esa fuente y se ejecutó como proceso
separado contra tablas log y KV del fixture DML aislado: conteos esperados 6 y 1,
pool 16 MiB/particiones destino 2, limpieza contexto/conexión correcta. Caso DML/
ejemplo pasó en 12,25 s. Clippy all-target raíz con warnings denegados pasó offline
sobre fuente final (2m27s); comprobaciones árbol original pasaron.

Comandos ejecutados tras compilar ejemplo en esa copia de fuentes:

```sh
CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/opencode/native-repro-target-a7739086 cargo build -p fluss-datafusion --offline --locked --example native_query
FLUSS_NATIVE_QUERY_EXAMPLE=/tmp/opencode/native-repro-target-a7739086/debug/examples/native_query DATAFUSION_POOL_MIB=16 DATAFUSION_TARGET_PARTITIONS=2 CARGO_BUILD_JOBS=8 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/opencode/native-repro-target-a7739086 uv run --no-project --env-file /home/midnattsol/code/datalush/fluss/lab/.env cargo test -p fluss-datafusion --offline --locked --test write_sql insert_log_and_upsert_kv_from_sql -- --ignored --test-threads=1
```

Actualizar resultados documentales tras export no cambia el código nativo probado.
Tras añadir modo reader continuo medido, último export fue
`/tmp/opencode/native-source-8881fb542df84f43b689e4291533af43`, 4.284 archivos,
SHA256 `4ac0ada63aefeb5f7e635e62ad4f058772215117ef66a8738b47e50b31e82103`. Su clippy
all-target offline y build ejemplo pasaron con target aislado; perfil RELEASE nuevo
reader no acotado/INSERT pasó ejecución completa 60s/300s. Luego clippy identificó
expresión divisibilidad equivalente, cambiada a `is_multiple_of`; no cambió política
ni ruta de medición. Comprobaciones de fuentes y clippy all-target cliente/test-cluster,
formato y diff pasaron.

Estos exports no son checkout Git limpio de un commit autorizado: son evidencia
histórica de fuentes, no sustituto de verificación Git/perfiles limpios documentados abajo.

## Documentación canónica

- Contratos API/capacidades/entrega: `rust-contract.md`; detalle lectura/DML en
  `reading-semantics.md`, `delete-contract.md`, `merge-contract.md`.
- Observaciones/significado de métricas: `write-observation-contract.md`.
- Mapa aceptación motor: `native-engine-acceptance.md`.
- Evidencia permisos/fallos/pressure: `native-failure-verification.md`,
  `read-pressure-verification.md`, `write-pressure-verification.md`.
- Cargas/resultados medidos: `native-profile-plan.md`; resultados remotos/STS
  históricos están en `production-readiness.md` con alcance/modo de build reales.
- Entradas/comandos build/ejemplo: README raíz. Su alcance de aceptación nombra
  DataFusion nativo, no exige motor de trabajos Rust externo.

Este inventario resuelve disposiciones de limpieza nativa. Aceptación se sustenta en
fuente versionada, perfiles acotados y verificación final checkout limpio; release/
entrega fuera de ciclo nativo mantiene gates propios.

Serie versionada tiene [verificación Git limpia](native-checkout-verification.md).
Las comprobaciones enumeradas de código nativo/ejemplo/fallos pasaron. Fallo inicial
preflight RustFS se conserva como historia; smoke remoto sin cambios pasó después de
que usuario restauró endpoint existente. No se modificaron instalaciones/configuración
externas para lograrlo y no quedan fixtures propios.
