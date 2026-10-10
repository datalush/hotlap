# Hotlap — núcleo incremental: contrato y antecedentes técnicos

- Registro de contexto técnico: 2026-10-06
- Alcance actual: contrato `IncrementalCore` y operadores incrementales

## 1. Contrato actual

El motor mantiene estado incremental en sus operadores y usa `IncrementalCore`
como frontera propia. El contrato del núcleo es `Plan`, `TaskContext`, `ReadTask`
y lotes `ZSetBatch`; no depende del modelo de tareas de un framework incremental
externo. Los tipos del contrato no exponen `differential-dataflow`.

En producción, `hotlap-engine` mantiene estado columnar en sus operadores y
aplica cada lote como delta. La recomputación completa se usa como oráculo en
pruebas, no como ruta de producción. Arrow se usa en las fronteras.

## 2. Contexto histórico

Los experimentos, dependencias, prototipos y evidencia anteriores se conservan en
[antecedentes del núcleo incremental](hotlap-incremental-history.md).
