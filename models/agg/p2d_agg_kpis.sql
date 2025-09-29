{{ config(materialized='table') }}

with f as (
  select
    CUPS,
    date_trunc('day', FECHA_MEDIDA)                                                as FECHA,
    sum(nvl(MEDIDA_ACTIVA_ENTRANTE,0))                                            as ACTIVA_ENTRANTE_D,
    sum(nvl(MEDIDA_ACTIVA_SALIENTE,0))                                            as ACTIVA_SALIENTE_D,
    sum(nvl(MEDIDA_REACTIVA_CUADRANTE_1,0)
      + nvl(MEDIDA_REACTIVA_CUADRANTE_2,0)
      + nvl(MEDIDA_REACTIVA_CUADRANTE_3,0)
      + nvl(MEDIDA_REACTIVA_CUADRANTE_4,0))                                       as REACTIVA_D,
    max_by(VERANO, FECHA_MEDIDA)                                                  as VERANO_FLAG   -- si tu versión no tiene MAX_BY, ver macro abajo
  from {{ ref('p2d_dwh') }}
  where TIPO_MEDIDA = 11
  group by 1,2
),
k as (
  select
    CUPS,
    FECHA,
    (ACTIVA_ENTRANTE_D - ACTIVA_SALIENTE_D)                                       as ACTIVA_NETA_D,
    REACTIVA_D,
    iff(
      (ACTIVA_ENTRANTE_D - ACTIVA_SALIENTE_D) = 0 and REACTIVA_D = 0,
      null,
      least(1.0, greatest(0.0,
        (ACTIVA_ENTRANTE_D - ACTIVA_SALIENTE_D)
        / nullif(sqrt(power(ACTIVA_ENTRANTE_D - ACTIVA_SALIENTE_D,2)
                      + power(REACTIVA_D,2)), 0)
      ))
    )                                                                             as PF_D,
    VERANO_FLAG
  from f
),
r as (
  select
    CUPS, FECHA, ACTIVA_NETA_D, REACTIVA_D, PF_D, VERANO_FLAG,
    avg(ACTIVA_NETA_D) over (partition by CUPS order by FECHA rows between 6 preceding and current row)  as ACTIVA_NETA_R7,
    avg(ACTIVA_NETA_D) over (partition by CUPS order by FECHA rows between 29 preceding and current row) as ACTIVA_NETA_R30,
    avg(PF_D)            over (partition by CUPS order by FECHA rows between 29 preceding and current row) as PF_R30,
    (ACTIVA_NETA_D
     - avg(ACTIVA_NETA_D) over (partition by CUPS order by FECHA rows between 29 preceding and current row))
     / nullif(stddev(ACTIVA_NETA_D) over (partition by CUPS order by FECHA rows between 29 preceding and current row), 0) as Z30_ACTIVA
  from k
),
dim as (
  select
    r.*,
    iff(extract(dow from FECHA) in (0,6),'WEEKEND','WEEKDAY')                      as TIPO_DIA,
    iff(VERANO_FLAG=1,'VERANO','INVIERNO')                                        as ESTACION,
    extract(year  from FECHA)                                                     as ANIO,
    extract(month from FECHA)                                                     as MES,
    to_char(FECHA,'YYYY-MM')                                                      as ANIO_MES,
    to_char(FECHA,'IYYY-IW')                                                      as ISO_SEMANA
  from r
)
select
  *,
  current_timestamp() as TS_AGG
from dim
