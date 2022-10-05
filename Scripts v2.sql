-- Databricks notebook source
/* Estou selecionando toda a tabela para facilitar visualização de dados */
select * from data_science_fields_salary_categorization_csv

-- COMMAND ----------

/* para entender a quantidade de cargos que estão sendo analisados */

select cargo, count(1) from data_science_fields_salary_categorization_csv group by cargo
/* Conclui-se então que são 50 cargos diferentes */

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC ## 
-- MAGIC ##** Adicionando função is_numeric_type para retornar true/false
-- MAGIC ##
-- MAGIC 
-- MAGIC def is_numeric(s):
-- MAGIC     try:
-- MAGIC         float(s)
-- MAGIC         return True
-- MAGIC     except ValueError:
-- MAGIC         return False
-- MAGIC     

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ##
-- MAGIC ##**Adicionando o Boolean Type no PySpark
-- MAGIC ##
-- MAGIC from pyspark.sql.types import BooleanType

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ##
-- MAGIC ##** Registrando a função no SQL CONTEXT
-- MAGIC ##
-- MAGIC sqlContext.udf.register("is_numeric_type", is_numeric, BooleanType())

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ##
-- MAGIC ##**Função para contar se há informação diferente de número na coluna "Ano"
-- MAGIC ##
-- MAGIC sqlContext.sql("select count(1) as Qtde from data_science_fields_salary_categorization_csv where is_numeric_type(ano)=false").show()

-- COMMAND ----------

/* criando uma view para exportar ao Power BI*/
CREATE
OR REPLACE VIEW vw_trad as
SELECT
    idseq,
    ano,
    cargo,
    CASE
        WHEN nivel = "EX" THEN 'Diretor'
        WHEN nivel = "MI" THEN 'Pleno'
        WHEN nivel = "SE" THEN 'Senior'
        WHEN nivel = "EN" THEN 'Junior'
    END nivel,
    CASE
        WHEN tipo_de_contrato = 'FT' THEN 'Tempo Integral'
        WHEN tipo_de_contrato = 'PT' THEN 'Meio Periodo'
        WHEN tipo_de_contrato = 'CT' THEN 'Contrato'
        WHEN tipo_de_contrato = 'FL' THEN 'Freelance'
    END Tipo_de_Contrato,
    
    /* Converter o salario em rupias para dolar, transformar de string para decimal, multiplicar pela cotação (0,013 USD) */
    CAST(
        REPLACE(salary_in_rupees, ',', '') as DECIMAL(18, 2)) * 0.013 as Salario_USD,
        
    Employee_Location as Pais_Funcionario,
    Company_Location as Pais_Empresa,
    CASE
        WHEN Employee_Location <> Company_Location THEN 'Internacional'
        ELSE 'Local'
    END AS Empresa,
    CASE
        WHEN Company_Size = 'S' THEN 'até 50 func.'
        WHEN Company_Size = 'M' THEN 'de 51- 250 funcionarios'
        WHEN Company_Size = 'L' THEN 'mais de 250'
    end as Tamanho,
    CASE
        WHEN Remote_Working_Ratio = '0' THEN 'Presencial'
        WHEN Remote_Working_Ratio = '50' THEN 'Híbrido'
        WHEN Remote_Working_Ratio = '100' THEN 'Remoto'
    end as Formato_trabalho
FROM
    data_science_fields_salary_categorization_csv

-- COMMAND ----------

select * from vw_trad

-- COMMAND ----------


