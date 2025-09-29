{{ config(materialized='table') }}

with base as (
  select
    CUPS,
    date_trunc('day', FECHA_MEDIDA)                            as FECHA_D,
    MEDIDA_ACTIVA_ENTRANTE,
    MEDIDA_ACTIVA_SALIENTE,
    nvl(MEDIDA_REACTIVA_CUADRANTE_1,0)
  + nvl(MEDIDA_REACTIVA_CUADRANTE_2,0)
  + nvl(MEDIDA_REACTIVA_CUADRANTE_3,0)
  + nvl(MEDIDA_REACTIVA_CUADRANTE_4,0)                         as REACTIVA,
    CALIDAD_ACTIVA_ENTRANTE,
    CALIDAD_ACTIVA_SALIENTE
  from {{ ref('p2d_dwh') }}
  where TIPO_MEDIDA = 11
),
dia as (
  select
    CUPS,
    FECHA_D,
    count(*)                                                    as N_MUESTRAS,
    round(count(*) / 96.0, 4)                                   as COMPLETITUD_D,   -- ajusta si ≠ 15 min
    sum(iff(nvl(CALIDAD_ACTIVA_ENTRANTE,1)=0 and nvl(CALIDAD_ACTIVA_SALIENTE,1)=0,1,0)) as N_VALIDAS,
    sum(MEDIDA_ACTIVA_ENTRANTE)                                 as ACTIVA_ENTRANTE_D,
    sum(MEDIDA_ACTIVA_SALIENTE)                                 as ACTIVA_SALIENTE_D,
    sum(REACTIVA)                                               as REACTIVA_D
  from base
  group by 1,2
),
rules as (
  select
    *,
    iff(COMPLETITUD_D >= 0.90, 1, 0)                            as FLAG_OK_COMPLETITUD,
    iff(N_VALIDAS    >= 86,   1, 0)                             as FLAG_OK_CALIDAD, -- 90% de 96
    ACTIVA_ENTRANTE_D - ACTIVA_SALIENTE_D                       as ACTIVA_NETA_D
  from dia
)
select
  CUPS,
  FECHA_D                                        as FECHA,
  N_MUESTRAS,
  COMPLETITUD_D,
  FLAG_OK_COMPLETITUD,
  FLAG_OK_CALIDAD,
  ACTIVA_ENTRANTE_D,
  ACTIVA_SALIENTE_D,
  ACTIVA_NETA_D,
  REACTIVA_D,
  current_timestamp()                             as TS_AGG
from rules
where FLAG_OK_COMPLETITUD = 1 and FLAG_OK_CALIDAD = 1
