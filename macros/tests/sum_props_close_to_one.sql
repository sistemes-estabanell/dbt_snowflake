name: estabanell_p2d
version: 1.0.0
config-version: 2

# You’re on dbt Core 1.10.x
require-dbt-version: ">=1.10.0,<2.0.0"

# Must match the profile name in profiles.yml
profile: estabanell

# Project structure
model-paths: ["models"]
macro-paths: ["macros"]
seed-paths: ["seeds"]
test-paths: ["tests"]
analysis-paths: ["analysis"]
snapshot-paths: ["snapshots"]
docs-paths: ["models"]

# Snowflake-friendly quoting (leave identifiers unquoted unless needed)
quoting:
  database: false
  schema: false
  identifier: false
  column: false

# Helpful query tag for Snowflake auditing
query-comment:
  comment: "dbt {{ target.name }} | project estabanell_p2d | user {{ target.user }}"
  append: true

# Project-wide variables (you can wire these into the AGG SQL later)
vars:
  expected_samples_per_day: 96
  bands:
    valle: [0,1,2,3,4,5]
    llano: [6,7,8,9,10,11,12,13,14,15,16,17]
    punta: [18,19,20,21]
    postpunta: [22,23]
  proportions_sum_bounds:
    lower: 0.98
    upper: 1.02

models:
  +database: "{{ target.database }}"   # e.g. QH
  estabanell_p2d:
    # RAW is a source only (QH.P2D.P2D_RAW)
    sources:
      +schema: "P2D"

    dwh:
      +schema: "DWH"
      +materialized: table
      +tags: ["p2d","layer:dwh"]

    agg:
      +schema: "AGG"
      +materialized: table
      +tags: ["p2d","layer:agg"]

# Make tests error out by default
tests:
  +severity: error

# What `dbt clean` deletes
clean-targets:
  - "target"
  - "dbt_packages"
