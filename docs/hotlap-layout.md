# Estructura del proyecto Rust nativo Hotlap

Hotlap identifica el proyecto y el motor. Incluye su propio motor incremental
columnar (`hotlap-engine`) tras el contrato `hotlap-core`, con `hotlap` como
fachada pública; `hotlap-sql` y `hotlap-connectors` delimitan plan SQL/runtime.
DataFusion sigue siendo la base SQL/planificación/operadores/runtime de la
integración provider Fluss en `fluss-datafusion`; `fluss-rs` implementa cliente y
protocolo nativos. Los nombres de componentes y versiones API pública se
mantuvieron en la migración a Rust.

> El runtime Hotlap y la integración provider DataFusion nativa son capas distintas;
> aceptar providers no certifica comportamiento del runtime Hotlap.

## Árbol actual

```text
hotlap/
  Cargo.toml / Cargo.lock
  crates/hotlap/            # public facade over IncrementalCore
  crates/hotlap-core/       # engine contract (Plan, IncrementalCore, ZSetBatch)
  crates/hotlap-engine/     # Columnar incremental engine kernel
  crates/hotlap-sql/        # SQL to Plan boundary
  crates/hotlap-connectors/ # runtime/connector integration
  crates/fluss-datafusion/  # DataFusion Fluss provider integration
  clients/rust/
  vendor/datafusion-55.1.0/
  docs/
  LICENSE / NOTICE
```

Se retiraron árbol Java/referencia, bindings Python/C++/Elixir, sitio importado y
herramientas build/release propias de bindings. Git conserva su proveniencia de
importación. Se preservan avisos/licencias originales del código retenido. Los
archivos Java locales ignorados se archivaron fuera del repo, no se borraron.

El cliente Rust incluye `proto/FlussApi.proto` y código Rust generado versionado;
build normal no requiere Java. Regeneración usa esquema vendorizado, no fallback
a monorepo externo. Fixture STS corto ahora es Rust y reutiliza firma AWS CLI
contra RustFS existente, conservando contrato de 900 s/política. Hotlap no requiere
fuentes Python. Utilidades shell build/protocolo y credenciales externas Docker/
AWS CLI/lab siguen como dependencias de herramientas/runtime.

## Rutas e identidad

El directorio del proyecto pasa de `fluss-connectors` al hermano `hotlap`; rutas
hermanas `../lab/.env` siguen válidas. Documentos de evidencia histórica conservan
rutas/SHA/nombres de artefactos originales como historia, no instrucciones de
instalación actuales. Se preserva el historial del proyecto.
Worktrees Git enlazados se repararon tras mover directorio principal. Se conserva
remote Git configurado; esta operación no cambia nombre del repositorio GitHub.

Rutas de comandos actuales deben ser relativas a raíz Hotlap o usar directorio nuevo.
No hay symlink/fallback de compatibilidad en ruta anterior. Pruebas funcionales
nativas usan DEBUG/ocho jobs; perfiles, RELEASE/ocho jobs.

## Verificación de migración

Desde `/home/midnattsol/code/datalush/fluss/hotlap`, tras retirar árboles importados
y reparar worktrees enlazados:

- Workspace raíz: pasaron 29 pruebas core y 3 planner; integraciones opt-in compilan.
- Cliente: pasaron 836 pruebas, 2 ignoradas, tanto seriales como en última ejecución
  paralela predeterminada.
  La primera ejecución paralela observó fallo en contador de métricas (2 vs 1);
  reruns serial/paralelo predeterminado pasaron sin cambiar fuente cliente. Se
  conserva esta observación, sin afirmar que todas las corridas pasaron ni atribuir
  causa demostrada.
- Clippy all-target con warnings denegados del workspace raíz/cliente de cuatro
  miembros, formato y diff checks pasaron. Crate Rust `gen` compila con esquema
  vendorizado sin ruta/fallback Java.
- SQL native-sni: ocho casos escritura/DML/streaming pasaron (44,16s), incluido
  ejemplo query nativo compilado aparte contra log/KV desde ruta nueva.
- Endpoint STS Rust nuevo pasó **preflight** RustFS real (15,84s): servidor emitió
  credenciales de 900 s y primer scan remoto tuvo éxito. No afirma repetir prueba
  de expiración/renovación 900 s; evidencia histórica de expiración completa tiene
  alcance separado.
- Grafos locked resueltos siguen en 453 paquetes raíz / 431 cliente, DataFusion
  55.1/Arrow59 sin cambios y sin dependencias activas bindings no-Rust.
- Worktrees enlazados conservan sus HEAD y resuelven directorio Git común nuevo.
  El proyecto conserva su historial y binding predeterminado `hotlap`; se retiró el
  alias local obsoleto del directorio.

Las fuentes/artefactos locales ignorados retirados se conservan en
`/tmp/opencode/hotlap-retired-source`, incluido árbol Java, bindings, sitio,
herramientas y virtualenv/wheels/caches antiguos. El archivo no es fallback de build
ni entregable. Instalaciones externas `../lab` no se modificaron.

El cambio de nombre del directorio es una operación local del filesystem. Git
versiona fuentes Rust, marca y metadatos; el nombre del repositorio GitHub es otra
operación.
