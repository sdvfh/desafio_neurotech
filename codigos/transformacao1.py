import os
from pyspark.sql import SparkSession

# %%
spark = SparkSession.builder.getOrCreate()

diretorio = '/home/sergio/Documentos/neurotech/bases'

# %%
spark.read.csv(diretorio, pathGlobFilter='*_CONS.csv', recursiveFileLookup=True,
               header=True).withColumnRenamed('#ID_EVENTO', 'ID_EVENTO').createOrReplaceTempView('CONS')

spark.read.csv(diretorio, pathGlobFilter='*_DET.csv', recursiveFileLookup=True,
               header=True).withColumnRenamed('#ID_EVENTO', 'ID_EVENTO').createOrReplaceTempView('DET')
# %%
spark.sql('''
SELECT CONS.ID_EVENTO,
       CONS.ID_PLANO,
       CONS.IDADE_BENEFICIARIO,
       CONS.SEXO_BENEFICIARIO,
       CONS.CD_MUNIC_BENEFICIARIO,
       CONS.PORTE_OPERADORA,
       CONS.CD_MODALIDADE_OPERADORA,
       CONS.MODALIDADE_OPERADORA,
       CONS.CD_MUNIC_PRESTADOR,
       CONS.UF_PRESTADOR,
       CONS.DT_INTERNACAO,
       CONS.DT_SAIDA_INTERNACAO,
       CONS.CARATER_ATENDIMENTO,
       CONS.TIPO_INTERNACAO,
       CONS.REGIME_INTERNACAO,
       CONS.MOTIVO_ENCERRAMENTO,
       CONS.CID_1,
       CONS.CID_2,
       CONS.CID_3,
       CONS.CID_4,
       CONS.NR_DIARIAS_ACOMPANHANTE,
       CONS.NR_DIARIAS_UTI,
       CONS.LG_VALOR_PREESTABELECIDO,
       CONS.LG_OUTLIER,
       DET.CD_TUSS_PROCEDIMENTO,
       DET.QT_PROCEDIMENTO,
       DET.VL_PROCEDIMENTO,
       DET.VL_PAGO_FORNECEDOR,
       DET.CD_TABELA_REFERENCIA,
       DET.LG_PACOTE,
       DET.IND_TABELA_PROPRIA,
       DET.DT_INICIO_EVENTO,
       DET.UF_PRESTADOR AS UF_PRESTADOR_DET,
       DET.LG_OUTLIER AS LG_OUTLIER_DET
FROM CONS
LEFT JOIN DET ON DET.ID_EVENTO = CONS.ID_EVENTO
''').coalesce(1).write.csv(os.path.join(diretorio, 'ans_completo'), header=True)
