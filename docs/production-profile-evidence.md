# Evidencia histórica del perfil de presión de recursos

Resultados de referencia históricos, limitados a Rust/DataFusion, Docker Fluss y
RustFS. Los parámetros operativos vigentes están en
[preparación para producción](production-readiness.md).

## Perfil de referencia de presión de recursos

La prueba ignorada `datafusion_resource_pressure_rustfs` crea **su propio** cluster
Docker Fluss y prefijo único removible en bucket RustFS existente. Usa provider
DataFusion Rust nativo. El perfil default escribe 4.800 filas log de 128 KiB
(600 MiB decodificados, mayor al pool 512 MiB) y 64 filas KV (8 MiB). Ejecuta cuatro
consultas concurrentes (dos log, dos KV), dos particiones físicas por consulta.
Usa pool DataFusion compartido de 512 MiB, chunks remotos 1 MiB, dos operaciones
lectura y dos descargas por scanner, dos segmentos precargados/64 MiB por scanner.
Tras cinco minutos de calentamiento mide 30 min; completa cada ola antes de iniciar
otra. Cada scan verifica IDs, valores y duplicados; cada ola ejercita cancelación
temprana y comprueba liberar reservas Arrow/archivos temporales. También fuerza
rechazo explícito de pool de memoria aparte, diminuto.

En máquina de referencia con proceso fijado a cuatro CPUs permitidas
(`taskset -c 0-3`), pasaron dos ejecuciones completas. Última midió 112 scans en
1.854 s, 832.049.024 bytes remotos, pico RSS muestreado 170 MiB, high-water RSS
kernel ~169 MiB, pico pool 19 MiB y temporales remotos 636.819 bytes. RSS tras
calentamiento 120 MiB y tras medición 122 MiB. Prueba rechaza RSS >1,5 GiB,
temporales >2 GiB, reservas/temporales no cero por ola o crecimiento RSS >256 MiB
tras calentamiento. Presupuesto 2 GiB se **comprobó con RSS/high-water**, no fue
límite cgroup; temporales solo cubren scanner cliente, no disco de proceso entero.
Mediciones RSS son observaciones kernel separadas, redondeadas a MiB; diferencia
de 1 MiB no establece orden exacto de picos.

Son mediciones del baseline DEBUG histórico. Aceptación de rendimiento de ruta final
usa RELEASE; el [plan de perfiles nativos](native-profile-plan.md) fija carga/límites
antes de medir. No compare modos build como si solo cambiara el conector.

Ejecute perfil largo en solitario, con cuatro CPUs permitidas por afinidad host:

```bash
FLUSS_IMAGE=ghcr.io/midnattsol/fluss FLUSS_VERSION=1.0.0-midnattsol.6 CARGO_BUILD_JOBS=8 uv run --no-project --env-file ../lab/.env taskset -c 0-3 cargo test -p fluss-datafusion --locked --release --test remote_retention datafusion_resource_pressure_rustfs -- --ignored --nocapture
```

Para smoke funcional antes de ejecutar 35 minutos, configure
`FLUSS_PRESSURE_ROWS=64 FLUSS_PRESSURE_WARMUP_SECS=2 FLUSS_PRESSURE_MEASURE_SECS=5`.
Salida se marca `full=false` y **no** satisface criterio de aceptación de recursos.

## Verificación pendiente antes de una afirmación general de producción

El perfil medido es evidencia para esta configuración **Rust, Docker Fluss, RustFS**,
no límite estricto de cada asignación transitoria ni de otro despliegue. Buffers fetch
comprimidos y OpenDAL quedan fuera del pool DataFusion; batch Arrow se decodifica antes
de reservar. Prueba failover native-sni cubre laboratorio con dos coordinadores, no
coordinador único del perfil Docker. Perfil HTTP corto verifica 503/transporte,
timeout/cancelación reales. Perfil STS aparte de 900 s verifica expiración real y
renovación exitosa en scan pausado; pruebas deterministas cubren renovación fallida
y deadline tras expiración. Despliegue `.6` debe configurar explícitamente
`s3.assumed.role.policy` para restringir sesiones STS firmadas desde raíz. Perfil
Rust/Docker/RustFS de 35 min es evidencia acotada, no aceptación completa del motor.
Verificaciones provider DataFusion nativo no certifican runtime Hotlap. Cubren
política de planificación/runtime del llamador, concurrencia, contrapresión source/
sink, cancelación/reejecución y recuperación. Trabajos/checkpoints/conciliación
persistentes son responsabilidad aplicación; este despliegue no requiere scheduler
nuevo. Otros perfiles requieren evidencia propia; bindings no son entrega activa ni requisito.
