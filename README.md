# dbt + Snowflake + GitHub – Test Project

Este proyecto es una **prueba de integración**:  
- Los datos viven en **Snowflake**.  
- Las transformaciones se hacen con **dbt** (en capas RAW → DWH → AGG).  
- **GitHub** sirve como punto central para versionar y colaborar.  

La idea es validar un flujo básico de trabajo:
1. Cargar datos crudos en Snowflake (RAW).  
2. Transformarlos y estandarizarlos en una capa intermedia (DWH).  
3. Crear agregados y métricas listas para análisis (AGG).  
4. Usar GitHub para guardar los modelos y coordinar cambios.  

### Cómo usarlo
```bash
# instalar dependencias
dbt deps

# ejecutar modelos
dbt run

# correr pruebas
dbt test
