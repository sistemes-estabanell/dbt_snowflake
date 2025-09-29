{{ config(materialized='table') }}

with src as (
  select * from {{ source('p2d','P2D_RAW') }}
)

select
  /* Identificación y tipado ligero */
  upper(trim(CUPS))::string                         as CUPS,
  TIPO_MEDIDA::number(38,0)                         as TIPO_MEDIDA,

  /* === FECHA/HORA: siempre desde el dato RAW === */
  FECHA_MEDIDA::timestamp_ntz                       as FECHA_MEDIDA,
  cast(FECHA_MEDIDA as date)                        as FECHA,
  extract(hour from FECHA_MEDIDA)                   as HORA_00_23,
  date_trunc('hour', FECHA_MEDIDA)                  as FECHA_HH,
  date_trunc('day',  FECHA_MEDIDA)                  as FECHA_DD,
  to_char(FECHA_MEDIDA,'YYYY-MM-DD"T"HH24:MI:SS')   as FECHA_ISO_NTZ,

  /* Resto de columnas (sin transformaciones de negocio) */
  VERANO::number(1,0)                               as VERANO,
  MEDIDA_ACTIVA_ENTRANTE::number(38,0)              as MEDIDA_ACTIVA_ENTRANTE,
  CALIDAD_ACTIVA_ENTRANTE::number(38,0)             as CALIDAD_ACTIVA_ENTRANTE,
  MEDIDA_ACTIVA_SALIENTE::number(38,0)              as MEDIDA_ACTIVA_SALIENTE,
  CALIDAD_ACTIVA_SALIENTE::number(38,0)             as CALIDAD_ACTIVA_SALIENTE,
  MEDIDA_REACTIVA_CUADRANTE_1::number(38,0)         as MEDIDA_REACTIVA_CUADRANTE_1,
  CALIDAD_REACTIVA_CUADRANTE_1::number(38,0)        as CALIDAD_REACTIVA_CUADRANTE_1,
  MEDIDA_REACTIVA_CUADRANTE_2::number(38,0)         as MEDIDA_REACTIVA_CUADRANTE_2,
  CALIDAD_REACTIVA_CUADRANTE_2::number(38,0)        as CALIDAD_REACTIVA_CUADRANTE_2,
  MEDIDA_REACTIVA_CUADRANTE_3::number(38,0)         as MEDIDA_REACTIVA_CUADRANTE_3,
  CALIDAD_REACTIVA_CUADRANTE_3::number(38,0)        as CALIDAD_REACTIVA_CUADRANTE_3,
  MEDIDA_REACTIVA_CUADRANTE_4::number(38,0)         as MEDIDA_REACTIVA_CUADRANTE_4,
  CALIDAD_REACTIVA_CUADRANTE_4::number(38,0)        as CALIDAD_REACTIVA_CUADRANTE_4,
  MEDIDA_RESERVA_1::number(38,0)                    as MEDIDA_RESERVA_1,
  CALIDAD_RESERVA_1::number(38,0)                   as CALIDAD_RESERVA_1,
  MEDIDA_RESERVA_2::number(38,0)                    as MEDIDA_RESERVA_2,
  CALIDAD_RESERVA_2::number(38,0)                   as CALIDAD_RESERVA_2,
  METODO_OBTENCION::number(38,2)                    as METODO_OBTENCION,
  FIRMEZA::number(38,2)                             as FIRMEZA,

  /* Sellos (solo reloj del sistema, no tocan FECHA_MEDIDA) */
  current_timestamp()                                as TS_DWH_INSERTION,
  current_timestamp()                                as TS_DWH_UPDATED
from src
