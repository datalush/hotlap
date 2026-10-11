# Integración recuperable del join factorizado con COUNT(*)

## Estado y propósito

Especificación pendiente de revisión escrita. La dirección arquitectónica ha
sido aprobada: activación explícita, checkpoints lógicos canónicos v5 y
fallback permanente por instancia. Este documento no autoriza implementar
ni activar la optimización; el plan de implementación se prepara después de
aprobar esta especificación.

El objetivo es trasladar la mejora algorítmica del piloto al grafo ejecutable
sin cambiar el resultado observable, el protocolo de recuperación ni el plan
lógico público. La compatibilidad de checkpoints es un requisito que deberá
demostrarse, no una propiedad ya certificada.

## Alcance

- Inner equijoin directo entre dos fuentes distintas.
- Una clave Int64 por lado; agrupación por esa clave y un único COUNT(*).
- Schemas Int64 fijos, incluyendo claves nullable y schemas de solo clave.
- Multiplicidades retenidas no negativas en la ruta optimizada; deltas signed.
- Conservación de la semántica actual de NULL, nombres, orden, tipos,
  nullability y pesos Z-set de las salidas.
- Sin filtros, proyecciones o agregados adicionales dentro del patrón fusionado.
  Una envoltura existente fuera del patrón no se elimina ni se reescribe.

No incluye otros tipos de join, otras agregaciones, cambios de arrangements,
formatos físicos nuevos, SPI público nuevo, dependencias nuevas ni garantías
de exactly-once entre sistemas. Tampoco implica superioridad frente a Flink,
certificación ante cortes de energía o mejora de memoria del engine completo.

## Decisión arquitectónica

La fusión es una estrategia física interna de un plan lógico que sigue
conteniendo GroupAggregate y Join. Retiene las filas de ambos arrangements y
mantiene resúmenes amplios por clave para calcular cambios de COUNT sin
enumerar pares del join.

Se descarta para esta fase un estado persistente específico de la fusión:
requeriría una nueva versión y una política de compatibilidad adicional.
La representación canónica existente facilita desactivar la optimización,
pero obliga a demostrar fidelidad de exportación e importación.

### Componentes y límites

1. **Selector del grafo:** reconoce solamente el patrón admitido cuando la
   opción del engine está habilitada. Los demás planes mantienen su ruta actual.
2. **Kernel factorizado:** prepara cambios y salidas, comprueba el dominio y
   confirma estado únicamente después de completar el trabajo fallible.
3. **Adaptador lógico:** exporta/importa los estados de Group y Join en el orden
   original y reconstruye resúmenes derivados sin materializar pares.
4. **Controlador de fallback:** reconstruye operadores actuales a partir del
   estado anterior al delta que no es elegible y conserva la ruta actual.

Estos componentes permanecen internos. No se convierte el módulo cfg(test)
en producción simplemente quitando su guarda: la separación entre soporte de
pruebas y kernel reutilizable debe quedar explícita en el plan posterior.

## Selección y configuración

La opción está desactivada por defecto. Solo habilita la selección automática
del patrón restringido; no amplía el dominio SQL admitido por el engine.

La selección distingue tres estados de ejecución:

- **Pendiente:** aún faltan schemas o entradas para inicializar ambos lados.
- **Factorizado:** schemas y estado retenido cumplen el dominio verificado.
- **Actual:** patrón incompatible, estado no elegible o fallback realizado.

El estado pendiente conserva exactamente el buffering del join actual. Los
schemas reales se validan antes de entrar en la ruta factorizada. Un schema
válido para la ruta actual pero no para la fusión selecciona la ruta actual;
un cambio inválido para la ruta actual no adquiere validez por esta opción.

La configuración de ejecución debe llegar tanto a la construcción normal del
grafo como a la reconstrucción del core durante restore. No se infiere de los
datos persistidos ni se añade al formato v5.

## Dominio numérico y actualización

Se conservan los controles conservadores del piloto: conteos amplios internos,
límites checked para resultados Int64, multiplicidades finales no negativas
y límites por fila que consideran los pesos absolutos originales antes de
cancelación. No se debilitan estos controles para aumentar la cobertura.

Todas las comprobaciones y conversiones fallibles de la ruta factorizada se
realizan antes de confirmar arrangements, resúmenes y contadores de trabajo.
NotEligible no produce salida ni modifica ese estado.

Los errores reales de almacenamiento, codec o construcción de salidas se
propagan; no se reinterpretan como un motivo benigno de fallback. La fusión
no modifica la política de overflow o de pesos de los operadores actuales.

## Fallback

Al recibir un delta no elegible, el controlador prepara un Join y un
GroupAggregate equivalentes al estado lógico anterior, incluidos schemas y
buffers relevantes. No reconstruye el producto cartesiano del join ni relee
fuentes para inicializarlos.

Si esa preparación falla, se conserva la instancia factorizada anterior y
se devuelve el error sin publicar salida. Si tiene éxito, se instala la
representación actual y se aplica el delta pendiente exactamente una vez.
Desde ese punto rigen las mismas reglas de éxito, error y mutación que en la
ruta actual; no se promete una nueva atomicidad global para esos operadores.

La transición no publica la imagen inicial del agregado, no duplica taps y
no avanza offsets adicionalmente. La instancia permanece en la ruta actual
hasta destruirla o restaurarla: no intenta fusionarse otra vez en cada delta.

El modo físico no se persiste. Tras un reinicio se vuelve a decidir según la
configuración y la elegibilidad del estado restaurado. Por tanto, permanente
significa permanente durante la vida de la instancia, no entre reinicios.

## Checkpoints canónicos v5

Una instancia fusionada exporta las entradas lógicas Group y Join y después
las entradas de sus fuentes, incluidas las posiciones stateless. Conserva la
cardinalidad y el orden completos del recorrido lógico original.

La exportación incluye arrangements, schemas de entrada originales,
acumuladores del agregado y deltas pendientes con los mismos codecs y
convenciones de representación de la ruta actual. Debe preservar también
la distinción entre estado sin inicializar, inicializado vacío y no vacío.

Para cada grupo del subconjunto admitido, la multiplicidad rows y el único
acumulador COUNT coinciden con el producto de conteos laterales de su clave.
Los schemas del agregado corresponden al schema lógico de salida del join,
no a un batch físico reducido de una sola clave.

Los resúmenes amplios no se serializan como otra fuente de verdad. Restore
valida primero el estado lógico y reconstruye dichos resúmenes desde las
filas retenidas, sin enumerar pares. Los buffers pendientes todavía no
aplicados no se incorporan a esos conteos.

Un estado válido para la ruta actual pero fuera del dominio factorizado se
restaura en la ruta actual. No se rechaza solo por tener pesos negativos o
superar los límites conservadores de la optimización. La validación adicional
de la fusión solo se aplica cuando el estado puede interpretarse dentro de
su dominio; no introduce restricciones generales a otros planes o estados.

Una inconsistencia detectada entre arrangements, grupos, schemas o salida
retenida provoca rechazo: no se sobrescriben acumuladores para ocultarla.
Las validaciones y la construcción del grafo recuperado ocurren antes de
promoción, pruning o efectos sobre sinks. La validación no interpreta deltas
pendientes como cambios ya confirmados.

## Criterios de aceptación

1. Con la opción desactivada se conserva el comportamiento y la ruta actuales.
2. Una consulta SQL representativa selecciona realmente la fusión cuando está
   habilitada; la prueba observa la ruta interna además del resultado.
3. Comparaciones diferenciales por frame y acumuladas contra Join seguido de
   GroupAggregate verifican schemas y pesos, no solamente valores escalares.
4. Casos nullable, claves en distintas posiciones, schemas de solo clave,
   duplicados ponderados, inserciones, retracciones y actualizaciones de ambos
   lados conservan resultados y ausencia/presencia de grupos.
5. Cada límite de elegibilidad causa fallback antes de aplicar el delta; ese
   delta se observa una sola vez, sin salida inicial ni replay de fuentes.
6. Fallos de preparación del fallback conservan el estado anterior. Los errores
   posteriores a la transición coinciden con la ruta actual.
7. Checkpoints actuales se recuperan con la opción habilitada, y checkpoints
   fusionados se recuperan con ella deshabilitada. Se comprueba equivalencia
   canónica de estados y comportamiento futuro, no solo lectura del archivo.
8. Ambos sentidos funcionan antes de inicializar, con un solo lado pendiente,
   con estado vacío, con filas retenidas y después de fallback.
9. Tras recovery, offsets y taps conservan el protocolo existente, no hay
   backfill y los siguientes cambios coinciden con la referencia.
10. Corrupción relevante se rechaza antes de promoción o efectos irreversibles;
    la prueba distingue corrupción de estado válido no elegible para la fusión.
11. Las suites completas del engine pasan en debug y release; el workspace
    pasa formato, clippy estricto y tests. Se limita Cargo a un proceso y ocho
    jobs/threads como máximo, usando el checkout y target existentes.
12. Las mediciones separan kernel e integración. Se miden selección real,
    coste de checkpoint/restore y transición de fallback; las cifras del
    piloto no se presentan como rendimiento end-to-end de esta integración.

## Entrega y revisión

La implementación futura debe dividirse en cambios pequeños con revisión
independiente y revisión final de conjunto. Antes de cada corrección se
captura evidencia RED auténtica; las limitaciones históricas de TDD del piloto
no se reetiquetan como evidencia previa de esta fase.

No se hacen merge, push, worktrees ni builds alternativos bajo /tmp. Los
registros y planes externos anteriores se preservan. La entrega requiere
evidencia de los criterios anteriores; si la fidelidad canónica no puede
demostrarse, se detiene la implementación y se revisa la arquitectura, sin
cambiar silenciosamente el formato ni activar la fusión por defecto.
