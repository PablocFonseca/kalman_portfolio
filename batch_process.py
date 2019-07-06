import pandas as pd
import numpy as np
import os
import gzip
import pickle
import re
import cycler
import tqdm
import tqdm

import seaborn as sns
sns.set_style('ticks')

from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

from sqlalchemy import create_engine


# Leitura do banco

data_dir = "./"
db_file = 'fundos.db'
engine = create_engine(
    "sqlite:///"+os.path.abspath(os.path.join(data_dir, db_file)))

data_inicio = '2009-02-26'
data_fim = '2017-12-31'
#cnpj_fundo = '12.055.107/0001-16'  # ALASK BLACK
save_fig_dir = "./figures"
os.makedirs(save_fig_dir, exist_ok=True)

sqlquery = "SELECT DISTINCT CNPJ_FUNDO from cda"
cnpjs = pd.read_sql_query(sqlquery, engine)['CNPJ_FUNDO'].iloc[21:]

with open(os.path.join(save_fig_dir,'info.csv'), 'w+') as f:
    line = ['cnpj_fundo', 'info_nome_fundo', 'info_corr_sem_modelo', 'info_ativos_sem_id_pivot_cols', 'info_peso_medio', 'info_num_sem_serie_retornos' ,'info_ativos_sem_serie_retornos']
    print(','.join([str(i) for i in line]), file=f)


for cnpj_fundo in tqdm.tqdm(cnpjs.values, total=len(cnpjs.values)):
    try:
        # cotas
        sqlquery = """
        select VL_QUOTA, DT_COMPTC from inf_diario
        where CNPJ_FUNDO = '{cnpj_fundo}'
        and DT_COMPTC >= '{data_inicio}'
        and DT_COMPTC <= '{data_fim}'
        """.format(cnpj_fundo=cnpj_fundo, data_inicio=data_inicio, data_fim=data_fim)
        cotas = pd.read_sql_query(sqlquery, engine, parse_dates=['DT_COMPTC']).set_index(
            "DT_COMPTC")['VL_QUOTA'].sort_index()

        # carteira e pesos
        sqlquery = """
        select * from cda
        where CNPJ_FUNDO = '{cnpj_fundo}'
        and DT_COMPTC >= '{data_inicio}'
        and DT_COMPTC <= '{data_fim}'
        """.format(cnpj_fundo=cnpj_fundo, data_inicio=data_inicio, data_fim=data_fim)
        carteira = pd.read_sql_query(sqlquery, engine, parse_dates=['DT_COMPTC'])
        pl = carteira.sort_values(['DT_COMPTC', 'FILE'])[
            'VL_PATRIM_LIQ'].fillna(method='bfill')
        carteira['VL_PATRIM_LIQ'] = pl
        carteira['peso'] = carteira['VL_MERC_POS_FINAL'].div(carteira['VL_PATRIM_LIQ'])

        carteira = carteira[carteira.FILE != 'PL']

        # Valores a pagar são negativos
        q = (carteira.TP_APLIC == 'Valores a pagar') | (
            carteira.TP_APLIC.str.lower().str.contains('obriga|lançad'))
        carteira.loc[q, ['VL_MERC_POS_FINAL', 'peso']] *= -1

        pivot_cols = carteira.CD_ATIVO\
            .fillna(carteira.CD_ISIN)\
            .fillna(carteira[carteira.FILE.str.contains('BLC_8') & carteira.TP_ATIVO.str.contains("Ação|Ações|Outr|Recibo|BDR", flags=re.IGNORECASE)].DS_ATIVO.str.extract(r'([A-Z]{4,5}\d+\d?)', expand=False))\
            .fillna(carteira[carteira.FILE.str.contains('BLC_8') & carteira.TP_ATIVO.str.contains("Opção|Opcões", flags=re.IGNORECASE)].DS_ATIVO.str.extract(r'([A-Z]{4,5}\d+\d?)', expand=False))\
            .fillna(carteira[carteira.TP_ATIVO.str.contains("Debênture", flags=re.IGNORECASE)].CD_INDEXADOR_POSFX)\
            .fillna(carteira[carteira.TP_APLIC.str.lower().isin(['disponibilidades', 'valores a pagar', 'valores a receber'])].TP_APLIC.str.upper())\
            .fillna(carteira[carteira.TP_ATIVO.str.contains('T.tulo p.blico federal', flags=re.IGNORECASE)].DS_ATIVO.str.extract("(BR..........)", expand=False))\
            .fillna(carteira[carteira.TP_ATIVO.str.contains('T.tulo p.blico federal', flags=re.IGNORECASE)].DS_ATIVO.str.extract(r'(\d{6})', expand=False))\
            .fillna(carteira.CNPJ_FUNDO_COTA)\
            .fillna(carteira.CD_ATIVO_BV_MERC)\
            .fillna(carteira[(carteira.TP_ATIVO.str.contains(r'Contrato Futuro.*', flags=re.IGNORECASE)) & (carteira.DS_ATIVO.str.contains('IND'))].DS_ATIVO.str.extract('(IND)', expand=False))\
            .fillna(carteira.DS_ATIVO)
        info_ativos_sem_id_pivot_cols = pivot_cols.isna().sum()
        #print("Ativos sem identificação:", pivot_cols.isna().sum())

        pesos = carteira.pivot_table(
            index='DT_COMPTC', columns=pivot_cols, values='peso', aggfunc='sum')

        info_peso_medio = pesos.sum(1).round(2).mean()
        info_nome_fundo = carteira.DENOM_SOCIAL.unique()[0]

        sqlquery = """
        SELECT CNPJ_FUNDO,DT_COMPTC, VL_QUOTA from inf_diario
        WHERE CNPJ_FUNDO in ('{cnpj_fundos}')
        and DT_COMPTC >= '{data_inicio}'
        and DT_COMPTC <= '{data_fim}'
        """.format(cnpj_fundos="','".join(pivot_cols.unique()), data_inicio=data_inicio, data_fim=data_fim)
        fundos_qry = pd.read_sql_query(sqlquery, engine, parse_dates=['DT_COMPTC'])

        # acoes
        sqlquery = """
        select * from cotacoes
        WHERE CODNEG in ('{ativos}')
        OR CODISI in ('{ativos}')
        and DATA >= '{data_inicio}'
        and DATA <= '{data_fim}'
        """.format(ativos="','".join(pivot_cols.unique()), data_inicio=data_inicio, data_fim=data_fim)
        acoes_qry = pd.read_sql_query(sqlquery, engine, parse_dates=['DATA'])

        # Titulos
        sqlquery = """
        select * from titulos_publicos
        WHERE CODISI in ('{ativos}')
        OR CODIGO in ('{ativos}')
        and DT_MOV >= '{data_inicio}'
        and DT_MOV <= '{data_fim}'
        """.format(ativos="','".join(pivot_cols.unique()), data_inicio=data_inicio, data_fim=data_fim)
        titulos_qry = pd.read_sql_query(sqlquery, engine, parse_dates=['DT_MOV'])

        # Futuros
        sqlquery = """
        select DATE as DT_MOV, substr(MERCADORIA,0,4) || 'F' || VENCIMENTO as CODIGO, PRECO_ATUAL/PRECO_ANTERIOR - 1 as PCT_CHANGE, VARIACAO, PRECO_ATUAL from futuros
        WHERE DT_MOV >= '{data_inicio}'
        and DT_MOV <= '{data_fim}'
        and substr(MERCADORIA,0,4) || 'F' || VENCIMENTO in ('{ativos}')
        """.format(data_inicio=data_inicio, data_fim=data_fim, ativos="','".join(pivot_cols.unique()))
        futuros_qry = pd.read_sql_query(sqlquery, engine, parse_dates=[
                                        'DT_MOV'])  # .pivot_table(index='DT_MOV', columns='CODIGO', values='PCT_CHANGE').groupby(dict(futuros_carteira), axis=1).mean()
        #futuros_qry[['MERCADORIA','MES_VENCIMENTO', 'ANO_VENCIMENTO']] = futuros_qry.CODIGO.str.extract(r"(.*)_(.)(..)")

        # Indice Futuro
        sqlquery = """
        select DATE as DT_MOV, MERCADORIA,substr(MERCADORIA,0,4) || 'F' || VENCIMENTO as CODIGO, PRECO_ATUAL/PRECO_ANTERIOR - 1 as PCT_CHANGE, VARIACAO, PRECO_ATUAL from futuros
        WHERE DT_MOV >= '{data_inicio}'
        and DT_MOV <= '{data_fim}'
        and MERCADORIA == 'IND - Ibovespa'
        """.format(data_inicio=data_inicio, data_fim=data_fim)
        ibovfut_query = pd.read_sql_query(sqlquery, engine, parse_dates=['DT_MOV'])

        # Calcula os retornos dos ativos e dos títulos

        retornos_acoes = acoes_qry.query("TPMERC == 10").pivot_table(index='DATA', columns='CODNEG', values='PREULT').div(100).pct_change()
        retornos_titulos = titulos_qry.pivot_table(index='DT_MOV', columns='CODISI', values='VALOR_PAR').pct_change()
        retornos_opcoes = acoes_qry.query("TPMERC != 10").pivot_table(index='DATA', columns='CODNEG', values='PREULT').div(100).pct_change()
        retornos_fundos = fundos_qry.pivot_table(index='DT_COMPTC', columns='CNPJ_FUNDO',values='VL_QUOTA').pct_change()
        retornos_futuros = futuros_qry.pivot_table(index='DT_MOV', columns='CODIGO', values='PCT_CHANGE')
        retornos_ibov = pd.Series(ibovfut_query.pivot_table(index='DT_MOV', columns='CODIGO', values='VARIACAO').div(100000).mean(1), name='IBOV')

        # tratamento outliers
        q = retornos_acoes < retornos_acoes.std()*3
        retornos_acoes = retornos_acoes[q].fillna(
            retornos_acoes.rolling(3, center=True).median())

        ### Ativos na carteira sem série de retornos

        sem_id = set(pivot_cols) - set(retornos_acoes) - set(retornos_titulos) - \
            set(retornos_opcoes) - set(retornos_fundos) - set(retornos_futuros)
        info_num_sem_serie_retornos = len(sem_id)
        info_ativos_sem_serie_retornos = '|'.join(sorted(sem_id))
        #print(len(sem_id), "ativos sem séries de retorno.", '\n\r', '\n, '.join(sorted(sem_id)))

        retornos_acoes_empresa = retornos_acoes.groupby(lambda s: s[0:4], axis=1).median()
        pesos_acoes_empresa = pesos.reindex(columns=retornos_acoes.columns).groupby(lambda s: s[0:4], axis=1).sum()

        retornos = pd.concat([
            retornos_acoes_empresa,
            retornos_titulos, 
            retornos_opcoes, 
            retornos_fundos, 
            retornos_futuros, 
            retornos_ibov,
            pd.DataFrame(0, index=cotas.index, columns=sem_id)
        ], axis=1)
        retornos

        pesos_cota = pd.concat([pesos_acoes_empresa, pesos.drop(retornos_acoes.columns, axis=1)], axis=1)

        info_corr_sem_modelo = pesos_cota.fillna(0).reindex(columns=retornos.columns, index=cotas.index).ffill().mul(retornos.reindex(cotas.index)).sum(1).corr(cotas.pct_change())

        peso_grp = carteira.pivot_table(index='DT_COMPTC', columns='TP_ATIVO', values='peso', aggfunc='sum')

        with open(os.path.join(save_fig_dir,'info.csv'), 'a+') as f:
            line = [cnpj_fundo, info_nome_fundo, info_corr_sem_modelo, info_ativos_sem_id_pivot_cols, info_peso_medio, info_num_sem_serie_retornos ,info_ativos_sem_serie_retornos]
            print(','.join([str(i) for i in line]), file=f)



        fig, (axu, axd) = plt.subplots(2, gridspec_kw=dict(hspace=0))
        peso_grp[(peso_grp > 0)].fillna(0).plot.area(figsize=(20, 10), ax=axu, legend=None, color=sns.color_palette('tab20'))
        peso_grp[(peso_grp < 0)].fillna(0).plot.area(figsize=(20, 10), ax=axd,color=sns.color_palette('tab20'))


        axu.set_xticks([])
        axd.set_ylim(-axu.get_ylim()[1], 0)
        axd.legend(ncol=2, loc='lower right', bbox_to_anchor=(1,0))

        sns.despine()

        fig.suptitle("{} - {}".format(cnpj_fundo , info_nome_fundo))
        fig_name = re.sub('[^\w\-_\. ]', '_',"{} - {}.png".format(cnpj_fundo , info_nome_fundo))
        fig.savefig(os.path.join(save_fig_dir, fig_name), dpi=200, transparent=False)
    except Exception as ex:
        with open(os.path.join(save_fig_dir,'error.txt'), 'a') as f:
            print(cnpj_fundo, ex, file=f)
        continue
        