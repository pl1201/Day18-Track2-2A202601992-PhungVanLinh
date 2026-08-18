# Evidence — lightweight path

Per `rubric.md`, submitting the lightweight-path evidence:

- **[tree_lakehouse.txt](tree_lakehouse.txt)** — `_lakehouse/` directory layout
  (Bronze / Silver / Gold Delta tables, Iceberg warehouses, scratch tables
  used by each notebook's mission), depth 3, generated with `find`.
- **[delta_log_00000000000000000000.json](delta_log_00000000000000000000.json)** —
  contents of the first commit (`00000000000000000000.json`) from
  `_lakehouse/bronze/llm_calls_raw/_delta_log/`, pretty-printed. Shows the
  `commitInfo`, `protocol`, `metaData`, and `add` actions for the initial
  200,000-row Bronze write (NB4).
