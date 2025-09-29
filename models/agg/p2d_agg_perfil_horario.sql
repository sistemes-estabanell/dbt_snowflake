{{ config(materialized='table') }}

with quarter as (
  select
    CUPS,
    date_trunc('day', FECHA_MEDIDA)               as FECHA,
    HORA_00_23                                     as HORA,
    nvl(MEDIDA_ACTIVA_ENTRANTE,0)
  - nvl(MEDIDA_ACTIVA_SALIENTE,0)                 as ACTIVA_NETA
  from {{ ref('p2d_dwh') }}
  where TIPO_MEDIDA = 11
),
por_hora as (
  select
    CUPS, FECHA, HORA,
    sum(ACTIVA_NETA) as ACTIVA_NETA_H
  from quarter
  group by 1,2,3
),
pivot_wide as (
  select *
  from por_hora
  pivot ( sum(ACTIVA_NETA_H) for HORA in (
    0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
  ))
),
bandas as (
  select
    CUPS,
    FECHA,
    "0","1","2","3","4","5","6","7","8","9","10","11",
    "12","13","14","15","16","17","18","19","20","21","22","23",
    nvl("0",0)+nvl("1",0)+nvl("2",0)+nvl("3",0)+nvl("4",0)+nvl("5",0)         as VALLE,
    nvl("6",0)+nvl("7",0)+nvl("8",0)+nvl("9",0)+nvl("10",0)+nvl("11",0)
      +nvl("12",0)+nvl("13",0)+nvl("14",0)+nvl("15",0)+nvl("16",0)+nvl("17",0) as LLANO,
    nvl("18",0)+nvl("19",0)+nvl("20",0)+nvl("21",0)                            as PUNTA,
    nvl("22",0)+nvl("23",0)                                                    as POSTPUNTA
  from pivot_wide
),
normalizado as (
  select
    *,
    nullif(
      nvl("0",0)+nvl("1",0)+nvl("2",0)+nvl("3",0)+nvl("4",0)+nvl("5",0)
      + nvl("6",0)+nvl("7",0)+nvl("8",0)+nvl("9",0)+nvl("10",0)+nvl("11",0)
      + nvl("12",0)+nvl("13",0)+nvl("14",0)+nvl("15",0)+nvl("16",0)+nvl("17",0)
      + nvl("18",0)+nvl("19",0)+nvl("20",0)+nvl("21",0)+nvl("22",0)+nvl("23",0)
    ,0) as SUM_DIA
  from bandas
)
select
  CUPS, FECHA,
  "0","1","2","3","4","5","6","7","8","9","10","11",
  "12","13","14","15","16","17","18","19","20","21","22","23",
  "0"/SUM_DIA  as P0,  "1"/SUM_DIA  as P1,  "2"/SUM_DIA  as P2,  "3"/SUM_DIA  as P3,
  "4"/SUM_DIA  as P4,  "5"/SUM_DIA  as P5,  "6"/SUM_DIA  as P6,  "7"/SUM_DIA  as P7,
  "8"/SUM_DIA  as P8,  "9"/SUM_DIA  as P9,  "10"/SUM_DIA as P10, "11"/SUM_DIA as P11,
  "12"/SUM_DIA as P12, "13"/SUM_DIA as P13, "14"/SUM_DIA as P14, "15"/SUM_DIA as P15,
  "16"/SUM_DIA as P16, "17"/SUM_DIA as P17, "18"/SUM_DIA as P18, "19"/SUM_DIA as P19,
  "20"/SUM_DIA as P20, "21"/SUM_DIA as P21, "22"/SUM_DIA as P22, "23"/SUM_DIA as P23,
  VALLE, LLANO, PUNTA, POSTPUNTA,
  VALLE/SUM_DIA as P_VALLE,
  LLANO/SUM_DIA as P_LLANO,
  PUNTA/SUM_DIA as P_PUNTA,
  POSTPUNTA/SUM_DIA as P_POSTPUNTA,
  current_timestamp() as TS_AGG
from normalizado
