# Hotlap — CLI: REPL `hotlap` (SP5)

- Fecha: 2026-10-08
- Estado: implementado (SP5), tests verdes
- Alcance: binario `hotlap` y crate `hotlap-cli`, un REPL sobre la API embebida.
- API: `docs/hotlap-api.md`. Diseño:
  `2026-10-08-hotlap-api-cli-observability-design.md` (local, fuera del repo).
- Kata: SP5.

> Código y comentarios en **inglés**; este documento en español.

## 1. Propósito

`hotlap-cli` es un **REPL tipo DuckDB** que ejecuta sentencias a través de
`hotlap_runtime::Session` e imprime resultados como tabla. **No tiene estado
propio**: toda la semántica (DDL, `START`, consultas, métricas, checkpoints)
vive en la sesión. El crate se divide en una biblioteca testeable
(`repl`, `render`, `bootstrap`) y un binario fino (`main.rs`) que parsea flags
y arranca la sesión.

## 2. Uso

```text
hotlap [--state-dir DIR] [--bootstrap HOST] [--checkpoint-interval SECS]
```

| Flag | Efecto |
| --- | --- |
| `--state-dir DIR` | Directorio para el estado durable de checkpoints (`DurableStateBackend`). |
| `--bootstrap HOST` | Bootstrap Fluss por defecto para las DDL que lo omitan. |
| `--checkpoint-interval SECS` | Intervalo de checkpoint periódico (segundos). |
| `-h` / `--help` | Imprime el uso y sale con éxito. |

Reglas de composición:

- `--bootstrap` envuelve las factorías Fluss por defecto
  (`BootstrapSourceFactory`/`BootstrapSinkFactory`): rellena la opción
  `bootstrap` de cada `CREATE SOURCE`/`CREATE SINK` solo si la DDL no la trae.
- `--state-dir` con o sin intervalo habilita checkpointing; **si solo se da
  `--state-dir`**, el intervalo por defecto es **60 s** y se conservan los
  **3** checkpoints más nuevos.
- `--checkpoint-interval` **sin** `--state-dir` usa un backend **en memoria**.
- Sin flags de checkpoint, `CHECKPOINT` falla con
  `error: engine: checkpointing is not configured`.

## 3. Entrada y separación de sentencias

El REPL lee **línea a línea** de stdin:

- Si stdin es una terminal, escribe el prompt `hotlap> ` antes de cada línea;
  si es un pipe/redirección, **no** hay prompt.
- Cada línea se parte por `;`; cada segmento se recorta (`trim`) y los vacíos
  se ignoran. Por tanto se admiten **varias sentencias por línea** separadas
  por `;`.
- El `;` final es **opcional**: el fin de línea también termina la sentencia.
- Metacomandos (`\q`, `\?`) se reconocen como segmento, con o sin `;`.

> **Límite:** las sentencias **no pueden ocupar varias líneas**. Cada línea se
> procesa de forma independiente; una DDL repartida en dos líneas fallará. No
> hay historial, edición multilínea ni TUI.

## 4. Comandos

| Comando | Descripción |
| --- | --- |
| `<sql>` | `CREATE SOURCE` / `CREATE MATERIALIZED VIEW` / `CREATE SINK` / `START` / `SELECT …`. |
| `CHECKPOINT` | Toma un checkpoint ahora e imprime `checkpoint <id>`. Insensible a mayúsculas. |
| `SHOW METRICS` | Imprime las métricas como tabla `name`/`value`. Insensible a mayúsculas. |
| `\q` | Sale del REPL. Insensible a mayúsculas. |
| `\?` | Muestra la ayuda. |

## 5. Ejemplos

Sesión interactiva:

```console
$ hotlap --bootstrap localhost:9123 --state-dir ./hotlap-state
hotlap> CREATE SOURCE src WITH (connector='fluss', 'table.name'='events')
        WATERMARK FOR _event_time AS _event_time - INTERVAL '1 s';
```

Como el REPL no admite sentencias multilínea, en la práctica se escribe cada
sentencia en **una** línea:

```console
$ hotlap --bootstrap localhost:9123 --state-dir ./hotlap-state
hotlap> CREATE SOURCE src WITH (connector='fluss', 'table.name'='events') WATERMARK FOR _event_time AS _event_time - INTERVAL '1 s';
hotlap> CREATE MATERIALIZED VIEW mv AS SELECT k, count(*) FROM src GROUP BY k, tumble(_event_time, INTERVAL '10 s');
hotlap> START;
START
hotlap> SELECT k, count FROM mv;
+---+-------+
| k | count |
+---+-------+
| 1 | 3     |
+---+-------+
hotlap> SHOW METRICS;
+------------------+-------+
| name             | value |
+------------------+-------+
| rows_ingested    | 5     |
| rows_emitted     | 1     |
+------------------+-------+
hotlap> CHECKPOINT;
checkpoint 1
hotlap> \q
```

Script por pipe (sin prompt), ideal para pruebas de humo:

```console
$ printf '%s\n' \
    "CREATE SOURCE src WITH (connector='inmem') WATERMARK FOR _event_time AS _event_time - INTERVAL '1 s';" \
    "CREATE MATERIALIZED VIEW mv AS SELECT k, count(*) FROM src GROUP BY k, tumble(_event_time, INTERVAL '10 s');" \
    "START;" \
    "SELECT k, count FROM mv;" \
    "SHOW METRICS;" \
    '\q' \
  | hotlap
```

## 6. Salida y errores

- **`SELECT`**: tabla alineada y con bordes (Arrow `pretty_format_batches`).
- **DDL/`START`**: se imprime el mensaje de acuse (`Ack`) si no está vacío
  (p. ej. `START`).
- **Error de SQL**: se imprime `error: <mensaje>` y el REPL **continúa** con la
  siguiente sentencia (una consulta rota no tumba la sesión).
- **Error de comando** (`CHECKPOINT`, `SHOW METRICS` o fallo de E/S): aborta el
  REPL; el binario imprime `hotlap: <error>` por stderr y sale con código de
  fallo.

## 7. Límites (no-goals v1)

- Una sentencia por línea; sin SQL multilínea.
- Sin historial, autocompletado ni TUI.
- Una sola sesión por proceso (mono-proceso).
- Sin control remoto: el REPL solo habla con su `Session` embebida.
