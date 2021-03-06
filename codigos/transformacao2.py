import os
from pyspark.sql import SparkSession

# %%
spark = SparkSession.builder.getOrCreate()

diretorio = '/home/sergio/Documentos/neurotech/bases'

# %%
spark.read.csv(os.path.join(diretorio, 'ans_completo'), header=True).createOrReplaceTempView('ans')

# %%
spark.sql('''
WITH QUERY AS (

	SELECT ID_EVENTO,
		   datediff(to_date(DT_SAIDA_INTERNACAO, 'dd/mm/yyyy'),to_date(DT_INTERNACAO, 'dd/mm/yyyy')) AS QT_DIAS_INTERNACAO,

		   CAST(IDADE_BENEFICIARIO AS INTEGER)                                                       AS QT_IDADE_BENEFICIARIO,
		   CASE SEXO_BENEFICIARIO WHEN 'F' THEN 1 WHEN 'M' THEN 0 END                                AS SN_FEMININO,

		   CASE WHEN SUBSTRING(CD_MUNIC_BENEFICIARIO, 1, 2) = 11 THEN 1 ELSE 0 END                   AS SN_ESTADO_BENEFICIARIO_RONDONIA,
		   CASE WHEN SUBSTRING(CD_MUNIC_BENEFICIARIO, 1, 2) = 12 THEN 1 ELSE 0 END                   AS SN_ESTADO_BENEFICIARIO_ACRE,
		   CASE WHEN SUBSTRING(CD_MUNIC_BENEFICIARIO, 1, 2) = 13 THEN 1 ELSE 0 END                   AS SN_ESTADO_BENEFICIARIO_AMAZONAS,
		   CASE WHEN SUBSTRING(CD_MUNIC_BENEFICIARIO, 1, 2) = 14 THEN 1 ELSE 0 END                   AS SN_ESTADO_BENEFICIARIO_RORAIMA,
		   CASE WHEN SUBSTRING(CD_MUNIC_BENEFICIARIO, 1, 2) = 15 THEN 1 ELSE 0 END                   AS SN_ESTADO_BENEFICIARIO_PARA,
		   CASE WHEN SUBSTRING(CD_MUNIC_BENEFICIARIO, 1, 2) = 16 THEN 1 ELSE 0 END                   AS SN_ESTADO_BENEFICIARIO_AMAPA,
		   CASE WHEN SUBSTRING(CD_MUNIC_BENEFICIARIO, 1, 2) = 17 THEN 1 ELSE 0 END                   AS SN_ESTADO_BENEFICIARIO_TOCANTINS,

		   CASE WHEN SUBSTRING(CD_MUNIC_BENEFICIARIO, 1, 2) = 21 THEN 1 ELSE 0 END                   AS SN_ESTADO_BENEFICIARIO_MARANHAO,
		   CASE WHEN SUBSTRING(CD_MUNIC_BENEFICIARIO, 1, 2) = 22 THEN 1 ELSE 0 END                   AS SN_ESTADO_BENEFICIARIO_PIAUI,
		   CASE WHEN SUBSTRING(CD_MUNIC_BENEFICIARIO, 1, 2) = 23 THEN 1 ELSE 0 END                   AS SN_ESTADO_BENEFICIARIO_CEARA,
		   CASE WHEN SUBSTRING(CD_MUNIC_BENEFICIARIO, 1, 2) = 24 THEN 1 ELSE 0 END                   AS SN_ESTADO_BENEFICIARIO_RIO_GRANDE_DO_NORTE,
		   CASE WHEN SUBSTRING(CD_MUNIC_BENEFICIARIO, 1, 2) = 25 THEN 1 ELSE 0 END                   AS SN_ESTADO_BENEFICIARIO_PARAIBA,
		   CASE WHEN SUBSTRING(CD_MUNIC_BENEFICIARIO, 1, 2) = 26 THEN 1 ELSE 0 END                   AS SN_ESTADO_BENEFICIARIO_PERNAMBUCO,
		   CASE WHEN SUBSTRING(CD_MUNIC_BENEFICIARIO, 1, 2) = 27 THEN 1 ELSE 0 END                   AS SN_ESTADO_BENEFICIARIO_ALAGOAS,
		   CASE WHEN SUBSTRING(CD_MUNIC_BENEFICIARIO, 1, 2) = 28 THEN 1 ELSE 0 END                   AS SN_ESTADO_BENEFICIARIO_SERGIPE,
		   CASE WHEN SUBSTRING(CD_MUNIC_BENEFICIARIO, 1, 2) = 29 THEN 1 ELSE 0 END                   AS SN_ESTADO_BENEFICIARIO_BAHIA,
    
		   CASE WHEN SUBSTRING(CD_MUNIC_BENEFICIARIO, 1, 2) = 31 THEN 1 ELSE 0 END                   AS SN_ESTADO_BENEFICIARIO_MINAS_GERAIS,
		   CASE WHEN SUBSTRING(CD_MUNIC_BENEFICIARIO, 1, 2) = 32 THEN 1 ELSE 0 END                   AS SN_ESTADO_BENEFICIARIO_ESPIRITO_SANTO,
		   CASE WHEN SUBSTRING(CD_MUNIC_BENEFICIARIO, 1, 2) = 33 THEN 1 ELSE 0 END                   AS SN_ESTADO_BENEFICIARIO_RIO_DE_JANEIRO,
		   CASE WHEN SUBSTRING(CD_MUNIC_BENEFICIARIO, 1, 2) = 35 THEN 1 ELSE 0 END                   AS SN_ESTADO_BENEFICIARIO_SAO_PAULO,

		   CASE WHEN SUBSTRING(CD_MUNIC_BENEFICIARIO, 1, 2) = 41 THEN 1 ELSE 0 END                   AS SN_ESTADO_BENEFICIARIO_PARANA,
		   CASE WHEN SUBSTRING(CD_MUNIC_BENEFICIARIO, 1, 2) = 42 THEN 1 ELSE 0 END                   AS SN_ESTADO_BENEFICIARIO_SANTA_CATARINA,
		   CASE WHEN SUBSTRING(CD_MUNIC_BENEFICIARIO, 1, 2) = 43 THEN 1 ELSE 0 END                   AS SN_ESTADO_BENEFICIARIO_RIO_GRANDE_DO_SUL,
                  
		   CASE WHEN SUBSTRING(CD_MUNIC_BENEFICIARIO, 1, 2) = 50 THEN 1 ELSE 0 END                   AS SN_ESTADO_BENEFICIARIO_MATO_GROSSO_DO_SUL,
		   CASE WHEN SUBSTRING(CD_MUNIC_BENEFICIARIO, 1, 2) = 51 THEN 1 ELSE 0 END                   AS SN_ESTADO_BENEFICIARIO_MATO_GROSSO,
		   CASE WHEN SUBSTRING(CD_MUNIC_BENEFICIARIO, 1, 2) = 52 THEN 1 ELSE 0 END                   AS SN_ESTADO_BENEFICIARIO_GOIAS,
		   CASE WHEN SUBSTRING(CD_MUNIC_BENEFICIARIO, 1, 2) = 53 THEN 1 ELSE 0 END                   AS SN_ESTADO_BENEFICIARIO_DISTRITO_FEDERAL,

		   CASE WHEN SUBSTRING(CD_MUNIC_BENEFICIARIO, 1, 2) != SUBSTRING(CD_MUNIC_PRESTADOR, 1, 2)
																				THEN 1 ELSE 0 END    AS SN_ESTADO_DIF_PRESTADOR_BENEFICIARIO,

		   CASE PORTE_OPERADORA WHEN 'PEQUENO' THEN 1 ELSE 0 END                                     AS SN_PORTE_PEQUENO,
		   CASE PORTE_OPERADORA WHEN 'MEDIO' THEN 1 ELSE 0 END                                       AS SN_PORTE_MEDIO,
		   CASE PORTE_OPERADORA WHEN 'GRANDE' THEN 1 ELSE 0 END                                      AS SN_PORTE_GRANDE,

		   CASE WHEN CD_MODALIDADE_OPERADORA IN (21, 55) THEN 1 ELSE 0 END                           AS SN_MODALIDADE_ADMINISTRADORA,
		   CASE WHEN CD_MODALIDADE_OPERADORA = 22 THEN 1 ELSE 0 END                                  AS SN_MODALIDADE_COOP_MEDICA,
		   CASE WHEN CD_MODALIDADE_OPERADORA = 23 THEN 1 ELSE 0 END                                  AS SN_MODALIDADE_COOP_ODONTO,
		   CASE WHEN CD_MODALIDADE_OPERADORA = 24 THEN 1 ELSE 0 END                                  AS SN_MODALIDADE_AUTOGESTAO,
		   CASE WHEN CD_MODALIDADE_OPERADORA = 25 THEN 1 ELSE 0 END                                  AS SN_MODALIDADE_MED_GRUPO,
		   CASE WHEN CD_MODALIDADE_OPERADORA = 26 THEN 1 ELSE 0 END                                  AS SN_MODALIDADE_ODONTO_GRUPO,
		   CASE WHEN CD_MODALIDADE_OPERADORA = 27 THEN 1 ELSE 0 END                                  AS SN_MODALIDADE_FILANTROPIA,
		   CASE WHEN CD_MODALIDADE_OPERADORA IN (28, 29) THEN 1 ELSE 0 END                           AS SN_MODALIDADE_SEGURADORA,

		   CASE
			WHEN CARATER_ATENDIMENTO IN ('E', 1) THEN 1
			WHEN CARATER_ATENDIMENTO IN ('U', 2) THEN 0 END                                          AS SN_CARATER_ATENDIMENTO_ELETIVA,
                                         
		   CASE WHEN TIPO_INTERNACAO = 1 THEN 1 ELSE 0 END                                           AS SN_INTERNACAO_CLINICA,
		   CASE WHEN TIPO_INTERNACAO = 2 THEN 1 ELSE 0 END                                           AS SN_INTERNACAO_CIRURGICA,
		   CASE WHEN TIPO_INTERNACAO = 3 THEN 1 ELSE 0 END                                           AS SN_INTERNACAO_OBSTETRICA,
		   CASE WHEN TIPO_INTERNACAO IN (4, 6) THEN 1 ELSE 0 END                                     AS SN_INTERNACAO_PEDIATRICA,
		   CASE WHEN TIPO_INTERNACAO IN (5, 7) THEN 1 ELSE 0 END                                     AS SN_INTERNACAO_PSIQUIATRICA,

		   CASE WHEN REGIME_INTERNACAO = 1 THEN 1 ELSE 0 END                                         AS SN_TP_INTERNACAO_HOSPITALAR,
		   CASE WHEN REGIME_INTERNACAO = 2 THEN 1 ELSE 0 END                                         AS SN_TP_INTERNACAO_HOSPITAL_DIA,
		   CASE WHEN REGIME_INTERNACAO = 3 THEN 1 ELSE 0 END                                         AS SN_TP_INTERNACAO_DOMICILIAR,

		   CASE WHEN 
					(MOTIVO_ENCERRAMENTO IN (1, 2, 3, 5, 71)) OR 
					(MOTIVO_ENCERRAMENTO BETWEEN 11 AND 19) OR 
					(MOTIVO_ENCERRAMENTO BETWEEN 61 AND 64) THEN 1 ELSE 0 END                        AS SN_ALTA,
		   CASE WHEN
					(MOTIVO_ENCERRAMENTO IN (6)) OR
					(MOTIVO_ENCERRAMENTO BETWEEN 41 AND 44) OR
					(MOTIVO_ENCERRAMENTO BETWEEN 52 AND 54) OR
					(MOTIVO_ENCERRAMENTO BETWEEN 65 AND 69) THEN 1 ELSE 0 END                        AS SN_OBITO,
		   CASE WHEN
					MOTIVO_ENCERRAMENTO = 10 OR
					(MOTIVO_ENCERRAMENTO BETWEEN 31 AND 39)
					 THEN 1 ELSE 0 END AS SN_TRANSFERENCIA,
		   CASE WHEN MOTIVO_ENCERRAMENTO IN (4) OR MOTIVO_ENCERRAMENTO BETWEEN 21 AND 28 
																				THEN 1 ELSE 0 END   AS SN_PERMANENCIA,
		   CASE WHEN MOTIVO_ENCERRAMENTO = 51 THEN 1 ELSE 0 END                                     AS SN_ENCERRAMENTO_ADM,
		   IND_TABELA_PROPRIA                                                                       AS SN_TABELA_PROPRIA,
		   NR_DIARIAS_ACOMPANHANTE                                                                  AS QT_DIARIAS_ACOMPANHANTE,
		   NR_DIARIAS_UTI                                                                           AS QT_DIARIAS_UTI,
		   LG_VALOR_PREESTABELECIDO                                                                 AS SN_VALOR_PREESTABELECIDO,

		   case when (SUBSTRING(CID_1, 1, 1) IN ('A', 'B') AND SUBSTRING(CID_1, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_2, 1, 1) IN ('A', 'B') AND SUBSTRING(CID_2, 2, 2) between 0 and 99) OR
					 (SUBSTRING(CID_3, 1, 1) IN ('A', 'B') AND SUBSTRING(CID_3, 2, 2) between 0 and 99) OR
					 (SUBSTRING(CID_4, 1, 1) IN ('A', 'B') AND SUBSTRING(CID_4, 2, 2) between 0 and 99) then 1 else 0 end   AS SN_CID_PARASITAS_INFECCAO,
		   CASE WHEN ((SUBSTRING(CID_1, 1, 1) = 'C' AND SUBSTRING(CID_1, 2, 2) BETWEEN 0 AND 99) OR (SUBSTRING(CID_1, 1, 1) = 'D' AND SUBSTRING(CID_1, 2, 2) BETWEEN 0 AND 48)) OR
					 ((SUBSTRING(CID_2, 1, 1) = 'C' AND SUBSTRING(CID_2, 2, 2) BETWEEN 0 AND 99) OR (SUBSTRING(CID_2, 1, 1) = 'D' AND SUBSTRING(CID_2, 2, 2) BETWEEN 0 AND 48)) OR
					 ((SUBSTRING(CID_3, 1, 1) = 'C' AND SUBSTRING(CID_3, 2, 2) BETWEEN 0 AND 99) OR (SUBSTRING(CID_3, 1, 1) = 'D' AND SUBSTRING(CID_3, 2, 2) BETWEEN 0 AND 48)) OR
					 ((SUBSTRING(CID_4, 1, 1) = 'C' AND SUBSTRING(CID_4, 2, 2) BETWEEN 0 AND 99) OR (SUBSTRING(CID_4, 1, 1) = 'D' AND SUBSTRING(CID_4, 2, 2) BETWEEN 0 AND 48)) then 1 else 0 end AS SN_CID_NEOPLASIAS,
		   CASE WHEN (SUBSTRING(CID_1, 1 , 1) = 'D' AND SUBSTRING(CID_1, 2, 2) between 50 and 89) OR 
					 (SUBSTRING(CID_2, 1 , 1) = 'D' AND SUBSTRING(CID_2, 2, 2) between 50 and 89) OR 
					 (SUBSTRING(CID_3, 1 , 1) = 'D' AND SUBSTRING(CID_3, 2, 2) between 50 and 89) OR 
					 (SUBSTRING(CID_4, 1 , 1) = 'D' AND SUBSTRING(CID_4, 2, 2) between 50 and 89) THEN 1 ELSE 0 END        AS SN_CID_HEMATOPOETICOS,
		   CASE WHEN (SUBSTRING(CID_1, 1 , 1) = 'E' AND SUBSTRING(CID_1, 2, 2) between 0 and 90) OR 
					 (SUBSTRING(CID_2, 1 , 1) = 'E' AND SUBSTRING(CID_2, 2, 2) between 0 and 90) OR 
					 (SUBSTRING(CID_3, 1 , 1) = 'E' AND SUBSTRING(CID_3, 2, 2) between 0 and 90) OR 
					 (SUBSTRING(CID_4, 1 , 1) = 'E' AND SUBSTRING(CID_4, 2, 2) between 0 and 90) THEN 1 ELSE 0 END         AS SN_CID_ENDOCRINAS,
		   CASE WHEN (SUBSTRING(CID_1, 1 , 1) = 'F' AND SUBSTRING(CID_1, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_2, 1 , 1) = 'F' AND SUBSTRING(CID_2, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_3, 1 , 1) = 'F' AND SUBSTRING(CID_3, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_4, 1 , 1) = 'F' AND SUBSTRING(CID_4, 2, 2) between 0 and 99) THEN 1 ELSE 0 END         AS SN_CID_TRANSTORNOS_MENTAIS,
		   CASE WHEN (SUBSTRING(CID_1, 1 , 1) = 'G' AND SUBSTRING(CID_1, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_2, 1 , 1) = 'G' AND SUBSTRING(CID_2, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_3, 1 , 1) = 'G' AND SUBSTRING(CID_3, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_4, 1 , 1) = 'G' AND SUBSTRING(CID_4, 2, 2) between 0 and 99) THEN 1 ELSE 0 END         AS SN_CID_SISTEMA_NERVOSO,
		   CASE WHEN (SUBSTRING(CID_1, 1 , 1) = 'H' AND SUBSTRING(CID_1, 2, 2) between 0 and 59) OR 
					 (SUBSTRING(CID_2, 1 , 1) = 'H' AND SUBSTRING(CID_2, 2, 2) between 0 and 59) OR 
					 (SUBSTRING(CID_3, 1 , 1) = 'H' AND SUBSTRING(CID_3, 2, 2) between 0 and 59) OR 
					 (SUBSTRING(CID_4, 1 , 1) = 'H' AND SUBSTRING(CID_4, 2, 2) between 0 and 59) THEN 1 ELSE 0 END         AS SN_CID_OLHOS,
		   CASE WHEN (SUBSTRING(CID_1, 1 , 1) = 'H' AND SUBSTRING(CID_1, 2, 2) between 60 and 95) OR 
					 (SUBSTRING(CID_2, 1 , 1) = 'H' AND SUBSTRING(CID_2, 2, 2) between 60 and 95) OR 
					 (SUBSTRING(CID_3, 1 , 1) = 'H' AND SUBSTRING(CID_3, 2, 2) between 60 and 95) OR 
					 (SUBSTRING(CID_4, 1 , 1) = 'H' AND SUBSTRING(CID_4, 2, 2) between 60 and 95) THEN 1 ELSE 0 END        AS SN_CID_OUVIDOS,
		   CASE WHEN (SUBSTRING(CID_1, 1 , 1) = 'I' AND SUBSTRING(CID_1, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_2, 1 , 1) = 'I' AND SUBSTRING(CID_2, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_3, 1 , 1) = 'I' AND SUBSTRING(CID_3, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_4, 1 , 1) = 'I' AND SUBSTRING(CID_4, 2, 2) between 0 and 99) THEN 1 ELSE 0 END         AS SN_CID_APARELHO_CIRCULATORIO,
		   CASE WHEN (SUBSTRING(CID_1, 1 , 1) = 'J' AND SUBSTRING(CID_1, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_2, 1 , 1) = 'J' AND SUBSTRING(CID_2, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_3, 1 , 1) = 'J' AND SUBSTRING(CID_3, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_4, 1 , 1) = 'J' AND SUBSTRING(CID_4, 2, 2) between 0 and 99) THEN 1 ELSE 0 END         AS SN_CID_APARELHO_RESPIRATORIO,
		   CASE WHEN (SUBSTRING(CID_1, 1 , 1) = 'K' AND SUBSTRING(CID_1, 2, 2) between 0 and 93) OR 
					 (SUBSTRING(CID_2, 1 , 1) = 'K' AND SUBSTRING(CID_2, 2, 2) between 0 and 93) OR 
					 (SUBSTRING(CID_3, 1 , 1) = 'K' AND SUBSTRING(CID_3, 2, 2) between 0 and 93) OR 
					 (SUBSTRING(CID_4, 1 , 1) = 'K' AND SUBSTRING(CID_4, 2, 2) between 0 and 93) THEN 1 ELSE 0 END         AS SN_CID_APARELHO_DIGESTIVO,
		   CASE WHEN (SUBSTRING(CID_1, 1 , 1) = 'L' AND SUBSTRING(CID_1, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_2, 1 , 1) = 'L' AND SUBSTRING(CID_2, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_3, 1 , 1) = 'L' AND SUBSTRING(CID_3, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_4, 1 , 1) = 'L' AND SUBSTRING(CID_4, 2, 2) between 0 and 99) THEN 1 ELSE 0 END         AS SN_CID_PELE,
		   CASE WHEN (SUBSTRING(CID_1, 1 , 1) = 'M' AND SUBSTRING(CID_1, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_2, 1 , 1) = 'M' AND SUBSTRING(CID_2, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_3, 1 , 1) = 'M' AND SUBSTRING(CID_3, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_4, 1 , 1) = 'M' AND SUBSTRING(CID_4, 2, 2) between 0 and 99) THEN 1 ELSE 0 END         AS SN_CID_OSTEOMUSCULAR,
		   CASE WHEN (SUBSTRING(CID_1, 1 , 1) = 'N' AND SUBSTRING(CID_1, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_2, 1 , 1) = 'N' AND SUBSTRING(CID_2, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_3, 1 , 1) = 'N' AND SUBSTRING(CID_3, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_4, 1 , 1) = 'N' AND SUBSTRING(CID_4, 2, 2) between 0 and 99) THEN 1 ELSE 0 END         AS SN_CID_APARELHO_GENIURINARIO,
		   CASE WHEN (SUBSTRING(CID_1, 1 , 1) = 'O' AND SUBSTRING(CID_1, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_2, 1 , 1) = 'O' AND SUBSTRING(CID_2, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_3, 1 , 1) = 'O' AND SUBSTRING(CID_3, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_4, 1 , 1) = 'O' AND SUBSTRING(CID_4, 2, 2) between 0 and 99) THEN 1 ELSE 0 END         AS SN_CID_GRAVIDEZ,
		   CASE WHEN (SUBSTRING(CID_1, 1 , 1) = 'P' AND SUBSTRING(CID_1, 2, 2) between 0 and 96) OR 
					 (SUBSTRING(CID_2, 1 , 1) = 'P' AND SUBSTRING(CID_2, 2, 2) between 0 and 96) OR 
					 (SUBSTRING(CID_3, 1 , 1) = 'P' AND SUBSTRING(CID_3, 2, 2) between 0 and 96) OR 
					 (SUBSTRING(CID_4, 1 , 1) = 'P' AND SUBSTRING(CID_4, 2, 2) between 0 and 96) THEN 1 ELSE 0 END         AS SN_CID_INFECCOES_PERINATAL,
		   CASE WHEN (SUBSTRING(CID_1, 1 , 1) = 'Q' AND SUBSTRING(CID_1, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_2, 1 , 1) = 'Q' AND SUBSTRING(CID_2, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_3, 1 , 1) = 'Q' AND SUBSTRING(CID_3, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_4, 1 , 1) = 'Q' AND SUBSTRING(CID_4, 2, 2) between 0 and 99) THEN 1 ELSE 0 END         AS SN_CID_MALFORMACAO_CONGENITA,
		   CASE WHEN (SUBSTRING(CID_1, 1 , 1) = 'R' AND SUBSTRING(CID_1, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_2, 1 , 1) = 'R' AND SUBSTRING(CID_2, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_3, 1 , 1) = 'R' AND SUBSTRING(CID_3, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_4, 1 , 1) = 'R' AND SUBSTRING(CID_4, 2, 2) between 0 and 99) THEN 1 ELSE 0 END         AS SN_CID_ACHADOS_ANORMAIS_EXAME,
		   CASE WHEN ((SUBSTRING(CID_1, 1, 1) = 'S' AND SUBSTRING(CID_1, 2, 2) BETWEEN 0 AND 99) OR (SUBSTRING(CID_1, 1, 1) = 'T' AND SUBSTRING(CID_1, 2, 2) BETWEEN 0 AND 98)) OR
					 ((SUBSTRING(CID_2, 1, 1) = 'S' AND SUBSTRING(CID_2, 2, 2) BETWEEN 0 AND 99) OR (SUBSTRING(CID_2, 1, 1) = 'T' AND SUBSTRING(CID_2, 2, 2) BETWEEN 0 AND 98)) OR
					 ((SUBSTRING(CID_3, 1, 1) = 'S' AND SUBSTRING(CID_3, 2, 2) BETWEEN 0 AND 99) OR (SUBSTRING(CID_3, 1, 1) = 'T' AND SUBSTRING(CID_3, 2, 2) BETWEEN 0 AND 98)) OR
					 ((SUBSTRING(CID_4, 1, 1) = 'S' AND SUBSTRING(CID_4, 2, 2) BETWEEN 0 AND 99) OR (SUBSTRING(CID_4, 1, 1) = 'T' AND SUBSTRING(CID_4, 2, 2) BETWEEN 0 AND 98)) then 1 else 0 end AS SN_CID_LESOES_ENVENENAMENTO,
		   CASE WHEN ((SUBSTRING(CID_1, 1, 1) = 'V' AND SUBSTRING(CID_1, 2, 2) BETWEEN 1 AND 99) OR (SUBSTRING(CID_1, 1, 1) = 'Y' AND SUBSTRING(CID_1, 2, 2) BETWEEN 0 AND 98)) OR
					 ((SUBSTRING(CID_2, 1, 1) = 'V' AND SUBSTRING(CID_2, 2, 2) BETWEEN 1 AND 99) OR (SUBSTRING(CID_2, 1, 1) = 'Y' AND SUBSTRING(CID_2, 2, 2) BETWEEN 0 AND 98)) OR
					 ((SUBSTRING(CID_3, 1, 1) = 'V' AND SUBSTRING(CID_3, 2, 2) BETWEEN 1 AND 99) OR (SUBSTRING(CID_3, 1, 1) = 'Y' AND SUBSTRING(CID_3, 2, 2) BETWEEN 0 AND 98)) OR
					 ((SUBSTRING(CID_4, 1, 1) = 'V' AND SUBSTRING(CID_4, 2, 2) BETWEEN 1 AND 99) OR (SUBSTRING(CID_4, 1, 1) = 'Y' AND SUBSTRING(CID_4, 2, 2) BETWEEN 0 AND 98)) then 1 else 0 end AS SN_CID_CAUSAS_EXTERNAS,
		   CASE WHEN (SUBSTRING(CID_1, 1 , 1) = 'Z' AND SUBSTRING(CID_1, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_2, 1 , 1) = 'Z' AND SUBSTRING(CID_2, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_3, 1 , 1) = 'Z' AND SUBSTRING(CID_3, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_4, 1 , 1) = 'Z' AND SUBSTRING(CID_4, 2, 2) between 0 and 99) THEN 1 ELSE 0 END         AS SN_CID_SERVICOS_SAUDE,
		   CASE WHEN (SUBSTRING(CID_1, 1 , 1) = 'U' AND SUBSTRING(CID_1, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_2, 1 , 1) = 'U' AND SUBSTRING(CID_2, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_3, 1 , 1) = 'U' AND SUBSTRING(CID_3, 2, 2) between 0 and 99) OR 
					 (SUBSTRING(CID_4, 1 , 1) = 'U' AND SUBSTRING(CID_4, 2, 2) between 0 and 99) THEN 1 ELSE 0 END         AS SN_CID_ESPECIAIS,

		   CASE WHEN CD_TABELA_REFERENCIA = 0 THEN 1 ELSE 0 END                                                            AS QT_TABELA_PROPRIA,
		   CASE WHEN CD_TABELA_REFERENCIA = 19 OR (CD_TABELA_REFERENCIA = 63 AND CD_TUSS_PROCEDIMENTO = 29) THEN 1 ELSE 0 END AS QT_USO_OPME,
		   CASE WHEN CD_TABELA_REFERENCIA = 20 OR (CD_TABELA_REFERENCIA = 63 AND CD_TUSS_PROCEDIMENTO = 30) THEN 1 ELSE 0 END AS QT_USO_MEDICAMENTO,
		   CASE WHEN CD_TABELA_REFERENCIA IN (22, 90, 98) AND SUBSTRING(CD_TUSS_PROCEDIMENTO, 1, 1) = 1 THEN 1 ELSE 0 END     AS QT_CONSULTAS,
		   CASE WHEN CD_TABELA_REFERENCIA IN (22, 90, 98) AND SUBSTRING(CD_TUSS_PROCEDIMENTO, 1, 1) = 2 THEN 1 ELSE 0 END     AS QT_PROC_CLINICO,
		   CASE WHEN CD_TABELA_REFERENCIA IN (22, 90, 98) AND SUBSTRING(CD_TUSS_PROCEDIMENTO, 1, 1) = 3 OR (CD_TABELA_REFERENCIA = 63 AND CD_TUSS_PROCEDIMENTO IN (2, 6)) THEN 1 ELSE 0 END AS QT_PROC_CIRURGICO,
		   CASE WHEN CD_TABELA_REFERENCIA IN (22, 90, 98) AND SUBSTRING(CD_TUSS_PROCEDIMENTO, 1, 1) = 4 OR (CD_TABELA_REFERENCIA = 63 AND (CD_TUSS_PROCEDIMENTO IN (1, 3, 4, 5, 8, 9, 10, 11, 13) OR CD_TUSS_PROCEDIMENTO BETWEEN 21 AND 28)) THEN 1 ELSE 0 END AS QT_PROC_DIAGNOSTICO,
		   CASE WHEN CD_TABELA_REFERENCIA IN (22, 90, 98) AND SUBSTRING(CD_TUSS_PROCEDIMENTO, 1, 1) = 5 OR (CD_TABELA_REFERENCIA = 63 AND (CD_TUSS_PROCEDIMENTO = 12 OR CD_TUSS_PROCEDIMENTO BETWEEN 14 AND 20)) THEN 1 ELSE 0 END AS QT_PROC_TERAPEUTICO,
		   CASE WHEN CD_TABELA_REFERENCIA IN (22, 90, 98) AND SUBSTRING(CD_TUSS_PROCEDIMENTO, 1, 1) = 8 OR (CD_TABELA_REFERENCIA = 63 AND CD_TUSS_PROCEDIMENTO = 7) THEN 1 ELSE 0 END AS QT_PROC_ODONTOLOGICO,
		   CASE WHEN CD_TABELA_REFERENCIA = 63 AND CD_TUSS_PROCEDIMENTO = 31 THEN 1 ELSE 0 END AS QT_TAXAS,

		   QT_PROCEDIMENTO,
		   VL_PROCEDIMENTO,
		   VL_PAGO_FORNECEDOR
    
FROM ANS
)

SELECT QT_DIAS_INTERNACAO,
       QT_IDADE_BENEFICIARIO,
       SN_FEMININO,
       SN_ESTADO_BENEFICIARIO_RONDONIA,
       SN_ESTADO_BENEFICIARIO_ACRE,
       SN_ESTADO_BENEFICIARIO_AMAZONAS,
       SN_ESTADO_BENEFICIARIO_RORAIMA,
       SN_ESTADO_BENEFICIARIO_PARA,
       SN_ESTADO_BENEFICIARIO_AMAPA,
       SN_ESTADO_BENEFICIARIO_TOCANTINS,
       SN_ESTADO_BENEFICIARIO_MARANHAO,
       SN_ESTADO_BENEFICIARIO_PIAUI,
       SN_ESTADO_BENEFICIARIO_CEARA,
       SN_ESTADO_BENEFICIARIO_RIO_GRANDE_DO_NORTE,
       SN_ESTADO_BENEFICIARIO_PARAIBA,
       SN_ESTADO_BENEFICIARIO_PERNAMBUCO,
       SN_ESTADO_BENEFICIARIO_ALAGOAS,
       SN_ESTADO_BENEFICIARIO_SERGIPE,
       SN_ESTADO_BENEFICIARIO_BAHIA,
       SN_ESTADO_BENEFICIARIO_MINAS_GERAIS,
       SN_ESTADO_BENEFICIARIO_ESPIRITO_SANTO,
       SN_ESTADO_BENEFICIARIO_RIO_DE_JANEIRO,
       SN_ESTADO_BENEFICIARIO_SAO_PAULO,
       SN_ESTADO_BENEFICIARIO_PARANA,
       SN_ESTADO_BENEFICIARIO_SANTA_CATARINA,
       SN_ESTADO_BENEFICIARIO_RIO_GRANDE_DO_SUL,
       SN_ESTADO_BENEFICIARIO_MATO_GROSSO_DO_SUL,
       SN_ESTADO_BENEFICIARIO_MATO_GROSSO,
       SN_ESTADO_BENEFICIARIO_GOIAS,
       SN_ESTADO_BENEFICIARIO_DISTRITO_FEDERAL,
       SN_MESMO_ESTADO_PRESTADOR_BENEFICIARIO,
       SN_MODALIDADE_ADMINISTRADORA,
       SN_MODALIDADE_COOP_MEDICA,
       SN_MODALIDADE_COOP_ODONTO,
       SN_MODALIDADE_AUTOGESTAO,
       SN_MODALIDADE_MED_GRUPO,
       SN_MODALIDADE_ODONTO_GRUPO,
       SN_MODALIDADE_FILANTROPIA,
       SN_MODALIDADE_SEGURADORA,
       SN_CARATER_ATENDIMENTO_ELETIVA,
       SN_INTERNACAO_CLINICA,
       SN_INTERNACAO_CIRURGICA,
       SN_INTERNACAO_OBSTETRICA,
       SN_INTERNACAO_PEDIATRICA,
       SN_INTERNACAO_PSIQUIATRICA,
       SN_TP_INTERNACAO_HOSPITALAR,
       SN_TP_INTERNACAO_HOSPITAL_DIA,
       SN_TP_INTERNACAO_DOMICILIAR,
       SN_ALTA,
       SN_OBITO,
       SN_PERMANENCIA,
       SN_ENCERRAMENTO_ADM,
       SN_TABELA_PROPRIA,
       QT_DIARIAS_ACOMPANHANTE,
       QT_DIARIAS_UTI,
       SN_VALOR_PREESTABELECIDO,
       SN_CID_PARASITAS_INFECCAO,
       SN_CID_NEOPLASIAS,
       SN_CID_HEMATOPOETICOS,
       SN_CID_ENDOCRINAS,
       SN_CID_TRANSTORNOS_MENTAIS,
       SN_CID_SISTEMA_NERVOSO,
       SN_CID_OLHOS,
       SN_CID_OUVIDOS,
       SN_CID_APARELHO_CIRCULATORIO,
       SN_CID_APARELHO_RESPIRATORIO,
       SN_CID_APARELHO_DIGESTIVO,
       SN_CID_PELE,
       SN_CID_OSTEOMUSCULAR,
       SN_CID_APARELHO_GENIURINARIO,
       SN_CID_GRAVIDEZ,
       SN_CID_INFECCOES_PERINATAL,
       SN_CID_MALFORMACAO_CONGENITA,
       SN_CID_ACHADOS_ANORMAIS_EXAME,
       SN_CID_LESOES_ENVENENAMENTO,
       SN_CID_CAUSAS_EXTERNAS,
       SN_CID_SERVICOS_SAUDE,
       SN_CID_ESPECIAIS,
       SUM(QT_TABELA_PROPRIA)      AS QT_TABELA_PROPRIA,
       SUM(QT_USO_OPME)            AS QT_USO_OPME,
       SUM(QT_USO_MEDICAMENTO)     AS QT_USO_MEDICAMENTO,
       SUM(QT_CONSULTAS)           AS QT_CONSULTAS,
       SUM(QT_PROC_CLINICO)        AS QT_PROC_CLINICO,
       SUM(QT_PROC_CIRURGICO)      AS QT_PROC_CIRURGICO,
       SUM(QT_PROC_DIAGNOSTICO)    AS QT_PROC_DIAGNOSTICO,
       SUM(QT_PROC_TERAPEUTICO)    AS QT_PROC_TERAPEUTICO,
       SUM(QT_PROC_ODONTOLOGICO)   AS QT_PROC_ODONTOLOGICO,
       SUM(QT_TAXAS)               AS QT_TAXAS,
       
       ROUND(SUM(QT_PROCEDIMENTO * VL_PROCEDIMENTO + VL_PAGO_FORNECEDOR), 2) AS VL_TOTAL
FROM QUERY
GROUP BY ID_EVENTO,
         QT_DIAS_INTERNACAO,
         QT_IDADE_BENEFICIARIO,
         SN_FEMININO,
         SN_ESTADO_BENEFICIARIO_RONDONIA,
         SN_ESTADO_BENEFICIARIO_ACRE,
         SN_ESTADO_BENEFICIARIO_AMAZONAS,
         SN_ESTADO_BENEFICIARIO_RORAIMA,
         SN_ESTADO_BENEFICIARIO_PARA,
         SN_ESTADO_BENEFICIARIO_AMAPA,
         SN_ESTADO_BENEFICIARIO_TOCANTINS,
         SN_ESTADO_BENEFICIARIO_MARANHAO,
         SN_ESTADO_BENEFICIARIO_PIAUI,
         SN_ESTADO_BENEFICIARIO_CEARA,
         SN_ESTADO_BENEFICIARIO_RIO_GRANDE_DO_NORTE,
         SN_ESTADO_BENEFICIARIO_PARAIBA,
         SN_ESTADO_BENEFICIARIO_PERNAMBUCO,
         SN_ESTADO_BENEFICIARIO_ALAGOAS,
         SN_ESTADO_BENEFICIARIO_SERGIPE,
         SN_ESTADO_BENEFICIARIO_BAHIA,
         SN_ESTADO_BENEFICIARIO_MINAS_GERAIS,
         SN_ESTADO_BENEFICIARIO_ESPIRITO_SANTO,
         SN_ESTADO_BENEFICIARIO_RIO_DE_JANEIRO,
         SN_ESTADO_BENEFICIARIO_SAO_PAULO,
         SN_ESTADO_BENEFICIARIO_PARANA,
         SN_ESTADO_BENEFICIARIO_SANTA_CATARINA,
         SN_ESTADO_BENEFICIARIO_RIO_GRANDE_DO_SUL,
         SN_ESTADO_BENEFICIARIO_MATO_GROSSO_DO_SUL,
         SN_ESTADO_BENEFICIARIO_MATO_GROSSO,
         SN_ESTADO_BENEFICIARIO_GOIAS,
         SN_ESTADO_BENEFICIARIO_DISTRITO_FEDERAL,
         SN_PORTE_PEQUENO,
         SN_PORTE_MEDIO,
         SN_PORTE_GRANDE,
         SN_MESMO_ESTADO_PRESTADOR_BENEFICIARIO,
         SN_MODALIDADE_ADMINISTRADORA,
         SN_MODALIDADE_COOP_MEDICA,
         SN_MODALIDADE_COOP_ODONTO,
         SN_MODALIDADE_AUTOGESTAO,
         SN_MODALIDADE_MED_GRUPO,
         SN_MODALIDADE_ODONTO_GRUPO,
         SN_MODALIDADE_FILANTROPIA,
         SN_MODALIDADE_SEGURADORA,
         SN_CARATER_ATENDIMENTO_ELETIVA,
         SN_INTERNACAO_CLINICA,
         SN_INTERNACAO_CIRURGICA,
         SN_INTERNACAO_OBSTETRICA,
         SN_INTERNACAO_PEDIATRICA,
         SN_INTERNACAO_PSIQUIATRICA,
         SN_TP_INTERNACAO_HOSPITALAR,
         SN_TP_INTERNACAO_HOSPITAL_DIA,
         SN_TP_INTERNACAO_DOMICILIAR,
         SN_ALTA,
         SN_OBITO,
         SN_PERMANENCIA,
         SN_ENCERRAMENTO_ADM,
         SN_TABELA_PROPRIA,
         QT_DIARIAS_ACOMPANHANTE,
         QT_DIARIAS_UTI,
         SN_VALOR_PREESTABELECIDO,
         SN_CID_PARASITAS_INFECCAO,
         SN_CID_NEOPLASIAS,
         SN_CID_HEMATOPOETICOS,
         SN_CID_ENDOCRINAS,
         SN_CID_TRANSTORNOS_MENTAIS,
         SN_CID_SISTEMA_NERVOSO,
         SN_CID_OLHOS,
         SN_CID_OUVIDOS,
         SN_CID_APARELHO_CIRCULATORIO,
         SN_CID_APARELHO_RESPIRATORIO,
         SN_CID_APARELHO_DIGESTIVO,
         SN_CID_PELE,
         SN_CID_OSTEOMUSCULAR,
         SN_CID_APARELHO_GENIURINARIO,
         SN_CID_GRAVIDEZ,
         SN_CID_INFECCOES_PERINATAL,
         SN_CID_MALFORMACAO_CONGENITA,
         SN_CID_ACHADOS_ANORMAIS_EXAME,
         SN_CID_LESOES_ENVENENAMENTO,
         SN_CID_CAUSAS_EXTERNAS,
         SN_CID_SERVICOS_SAUDE,
         SN_CID_ESPECIAIS
''').coalesce(1).write.csv(os.path.join(diretorio, 'ans_final'), header=True)
