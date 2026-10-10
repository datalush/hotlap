# Limpieza selectiva del historial Hotlap

La punta original publicada de `develop` era
`c0698eda43b1103aa618f7bc78e9d63f52a8fb05`. Equivalente filtrada:
`17af0dab9415deb4dfffe93f661e35f397ad97e9`. Antes de añadir esta documentación,
ambas puntas tenían idéntico tree hash:
`f8f3c82d442beaa8dfa55a9fd61e83bcade120aa` (406 archivos, 6.096.254 bytes).
Se conservaron los 39 commits de historial de fuentes; no se retiró del árbol actual
implementación Rust ni licencia.

Se retiraron de todas las versiones alcanzables: fuentes/site/herramientas Java
copiadas, bindings no-Rust importados y sus herramientas web/release, integración/
manifests/ejemplos/pruebas Python/FFI que antes estaban activos y helper STS Python
reemplazado. Core/backport DataFusion, cliente Fluss nativo, fixtures Rust,
LICENSE/NOTICE y evolución de fuente nativa permanecen. Commits anteriores son
snapshots históricos; filtrar rutas no garantiza que cada build antiguo siga
siendo ejecutable.

Pack de clon limpio filtrado: 1,48 MiB antes de esta documentación, frente a push
inicial original de 33,23 MiB. Versiones únicas alcanzables de archivos bajan de
4.510 / 71.830.830 bytes sin comprimir a 737 / 13.981.075 bytes. Son mediciones
distintas (tamaño pack vs blobs históricos sin comprimir), no tamaño checkout.

## Proveniencia histórica

La migración preservó historial de fuentes, nombres/timestamps de mediciones. El
filtrado cambió ancestry, no código nativo medido ni resultados pruebas. Hashes de
commits/archivos upstream terceros no cambiaron.

## Respaldo y migración

Directorio backup externo de esta operación:
`/home/midnattsol/code/datalush/fluss/hotlap-history-backup-20261005/`. Contiene
bundles Git local/remoto completos verificados, archivo de metadatos Git local
original y mapa de commits filtrados. Está deliberadamente fuera del repositorio;
no debe publicarse ni usarse como nueva ref en repositorio limpiado.

Solo se publicó `develop`; no había tags ni otras ramas remotas. Publicación usó
lease explícito para punta original, rechazando cambios remotos concurrentes.
Worktrees detached locales existentes se trasladaron a IDs mapeados. Reflogs antiguos
expiraron solo tras backup/migración y luego objetos viejos inalcanzables se limpiaron
localmente. GitHub puede retener objetos internos/inalcanzables por un tiempo; un
clone normal nuevo verifica el historial publicado alcanzable.

Clones viejos independientes deben reclonarse o migrarse con mapa SHA antes de
publicar ramas. No mezclar ramas de historial viejo ni crear tag remoto backup:
ambos podrían reintroducir historial retirado. Remote `upstream` legacy configurado
no se descargó ni publicó en esta operación.
