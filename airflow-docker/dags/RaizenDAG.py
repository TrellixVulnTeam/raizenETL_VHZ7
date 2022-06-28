from airflow import DAG
from airflow.operators.python import PythonOperator

import pandas as pd
import numpy as np

from datetime import datetime
from subprocess import  Popen

# função capturaDados/extração do arquivo XML
def capturaDados(nome, plan1): 
    for count, value in enumerate(nome):
        extrair = tabela(plan1[count])
        transformar = transformar(extrair)
        carrega = carregarDF(transformar,nome[count])  
    return None 

def tabela(plan1):
    primeiroDF=pd.read_csv(carregarDF,plan1).fillna(0)
    return primeiroDF

#função de transformar dados
def transformar(nome: str):
    dataFrame = pd.read_csv('C://Users//edlly//Downloads//raizenETL//vendas-combustiveis-m3', sheet_name=nome)
    dataFrame.columns = ['Combustível', 'Ano', 'Localidade', 'UF', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', 'TotalAno']
    
    dataFrame = dataFrame.melt(id_vars=['Combustível', 'Ano', 'Localidade', 'UF'], var_name='MES', value_name='Volume')
    dataFrame = dataFrame.loc[dataFrame['Variavel'] != 'TotalAno']
    
    dataFrame['year_month'] = dataFrame['Ano'].astype(str) + '-' + dataFrame['Variavel']
    dataFrame['year_month'] = pd.to_datetime(dataFrame['Mês/Ano'])
    
    dataFrame = dataFrame.drop(labels=['Variavel', 'Localidade', 'Ano'], axis=1)
    dataFrame.columns = ['product', 'uf', 'Volume', 'year_month']
    dataFrame['volume'] = pd.to_numeric(dataFrame['volume'])
    dataFrame = dataFrame.fillna(0)
    dataFrame['unit'] = 'm3'
    
    dfFinal = dfFinal.reset_index(drop=True)
    return None

def carregarDF(dataFrameFINAL: pd.DataFrame) -> pd.DataFrame:
    path=f'C://Users//edlly//Downloads//raizenETL//output'
    dataFrameFINAL.sort_values(by=['uf','product'])
    finalFile = '.csv'
    print(finalFile)
    dataFrameFINAL.to_csv(finalFile, header=True, index=False)
    output = dataFrameFINAL[dataFrameFINAL.find('(')+1:dataFrameFINAL.find(')')]
    return output 

if __name__ == "__main__":
    with DAG('raizenDAG', start_date = datetime(2022,6,27), schedule_interval = '30 * * * *', catchup = False) as dag:
        Extração = PythonOperator(
        task_id='Extração', 
        python_callable=capturaDados,
        dag=dag
        )
        
        derivateFuels = PythonOperator(
            task_id='LoadDerivados',
            python_callable=transformar,
            op_kwargs={'nome': ['derivados'],'folhas' : [1]},
            dag=dag
        )

        diesel = PythonOperator(
            task_id='LoadDiesel',
            python_callable = transformar,
            op_kwargs={'nome': ['diesel'],'folhas' : [2]},
            dag=dag
        )

Extração>>[derivateFuels, diesel]