import pandas as pd
import numpy as np
import requests
import random
import urllib
import json
import time
import sys
import random
import datetime
from datetime import date
from bs4 import BeautifulSoup
from selenium import webdriver
from twitter_api import TwitterClass
from pymongo import MongoClient
import re
import os


class TempoNaviosClass:
    """
    Classe de métodos auxiliares
    """
    def __init__(self):
        
        # flag usar IA
        self.flag_use_IA = 0
        
        # max cidades
        self.flag_max = 100
        
        # min
        self.min = 2
        
        # time sleep
        self.time_sleep = 5
        
        # API do Twitter
        self.twitter_api = TwitterClass()
        
        # get meses
        self.dict_map_mes = self.twitter_api.get_meses()
        
        # dia atual
        print (self.get_dia_atual())
        
        # path atual
        self.current_path = str(os.getcwd())
        
        # path do chromedriver
        self.path_to_chromedriver = os.path.join(self.current_path, 'chromedriver')
        
        # arquivos auxiliares
        self.path_infos_portos = os.path.join(self.current_path, "portos.csv")
        self.path_infos_cidades = os.path.join(self.current_path, "cidades.csv")
        self.path_bd = os.path.join(self.current_path, "cidades_bd.csv")
        path_credenciais_user_agent = os.path.join(self.current_path, "credenciais_user_agent.json")
        path_intents = os.path.join(self.current_path, "intents.json")
        path_analisador_lexico = os.path.join(self.current_path, "analisador_lexico.json")
        path_credenciais_mongodb = os.path.join(self.current_path, "credenciais_mongo.json")
        
        # leitura do arquivo json com credenciais mongodb
        f = open(path_credenciais_mongodb, encoding='utf-8', mode="r")
        dict_credenciais_mongodb = json.load(f)
        f.close()
        
        # credenciais
        password = dict_credenciais_mongodb['password']
        url_mongo = f'mongodb+srv://blab_reporter:{password}@blab.1g3izgz.mongodb.net/?retryWrites=true&w=majority'
        client = MongoClient(url_mongo)
        self.collection = client['BLAB']['BLAB']
        
        # leitura do arquivo json com as credenciais
        try:
            f = open(path_credenciais_user_agent, mode="r")
            infos_login = json.load(f)
            self.user_agent = infos_login['user_agent']
            f.close()
        except:
            self.user_agent = "temporary_agent"
            
        # parametros do webdriver
        self.chromeOptions = webdriver.ChromeOptions()
        self.chromeOptions.add_argument('--no-sandbox')
        self.chromeOptions.add_argument("--headless")
        self.chromeOptions.add_argument(f"user-agent={self.user_agent}")
        
        # leitura do arquivo json com os intents
        f = open(path_intents, encoding='utf-8', mode="r")
        self.dict_intents = json.load(f)
        f.close()
        
        # leitura do arquivo json com o analisador léxico
        f = open(path_analisador_lexico, mode="r")
        dict_json_lexico = json.load(f)
        f.close()
        self.dict_analisador_lexico = {}
        self.lista_palavras_analisador_lexico = set()
        for chave, valor in dict_json_lexico.items():
            palavra = chave.split(']')[1].strip()
            numero = chave.split('numero=')[1].split(',')[0].strip()
            genero = chave.split('genero=')[1].split(']')[0].strip()
            chave = f"{palavra}|{numero}|{genero}"
            self.dict_analisador_lexico[chave] = valor.strip()
            self.lista_palavras_analisador_lexico.add(palavra)
        self.lista_palavras_analisador_lexico = list(self.lista_palavras_analisador_lexico)
        
        # parâmetros
        self.url_tabua_mares = "https://www.tideschart.com"
        self.tempo_espera_tweet_segundos = 60
        self.qtd_cidades_selecionadas = 15
        self.qtd_min_dias_consecutivos = 5
        self.multiplicador_std = 1.3
        self.altura_mare_ruim = 1.6
        self.filler_tempo = 'céu nublado'
        self.modulo = 'tempo'
        
        # temperaturas max e min possíveis (validação conceitual)
        self.maior_temperatura_possivel = 55
        self.menor_temperatura_possivel = -10
        
        # icones
        self.icone_up = '▲'
        self.icone_down = '▼'
        
        # qtd de navios max e min possíveis (validação conceitual)
        self.maior_qtd_navios = 1_000
        self.menor_qtd_navios = 2

        # df cidades
        self.df_cidades = pd.read_csv(self.path_infos_cidades, encoding='latin-1', sep=',')
        
        # df portos
        self.df_portos = pd.read_csv(self.path_infos_portos, encoding='latin-1', sep=',')
        
        # colunas para atribuir valor
        self.lista_colunas_tempo = ['cidade',
                              'uf',
                              'tempo',
                              'temperatura',
                              'temperatura_max',
                              'temperatura_min',
                              'nebulosidade',
                              'umidade',
                              'vento',
                              'horario_por_sol',
                              'pesca',
                              'melhor_horario_pesca',
                              'altura_maior_onda',
                              'texto_onda']
        
        # colunas para salvar
        self.lista_colunas_navios = ['cidade',
                                     'qtd_navios_porto',
                                     'qtd_navios_chegando']
        
        # lista de colunas para salvar
        self.lista_colunas_salvar = ['cidade',
                              'uf',
                              'tempo',
                              'temperatura',
                              'temperatura_max',
                              'temperatura_min',
                              'nebulosidade',
                              'umidade',
                              'vento',
                              'horario_por_sol',
                              'pesca',
                              'melhor_horario_pesca',
                              'altura_maior_onda',
                              'texto_onda',
                              'qtd_navios_porto',
                              'qtd_navios_chegando',
                              'data']
        
        # se não existe arquivo de bd, cria
        if not os.path.exists(self.path_bd):
            pd.DataFrame(columns=self.lista_colunas_salvar).to_csv(self.path_bd, sep=';', index=False)
        
        # colunas de clima
        f = open("mapeamento_climas.json", mode="r", encoding="utf-8")
        self.dict_map_clima = json.load(f)
        f.close()
        
        # colunas de emoticons
        f = open("mapeamento_emoticons.json", mode="r", encoding="utf-8")
        self.dict_map_emoticons = json.load(f)
        f.close()
        
        # colunas de pesca
        self.dict_map_pesca = {'Today is an excellent fishing day': 'Excelente',
                               'Today is a good fishing day': 'Bom',
                               'Today is an average fishing day': 'Mediano'}
        
        # cidades
        self.lista_cidades_no = ['Rio de Janeiro']
        
        # path dos conteúdos do site
        self.path_url_imagem = '//*[@id="map"]'
        self.path_tempo1 = '//*[@id="main-content"]/div/div/div/div[1]/div[7]/div[1]/div/div[1]'
        self.path_temperatura1 = '//*[@id="main-content"]/div/div/div/div[1]/div[7]/div[2]/div/div[1]'
        self.path_temperatura_max_min1 = '//*[@id="main-content"]/div/div/div/div[1]/div[7]/div[2]/div/div[2]'
        self.path_nebulosidade1 = '//*[@id="main-content"]/div/div/div/div[1]/div[7]/div[1]/div/div[2]'
        self.path_umidade1 = '//*[@id="main-content"]/div/div/div/div[1]/div[7]/div[4]/div/div[1]'
        self.path_vento1 = '//*[@id="main-content"]/div/div/div/div[1]/div[7]/div[3]/div/div[1]'
        
        self.path_tempo2 = '//*[@id="main-content"]/div/div/div/div[1]/div[4]/div[1]/div/div[1]'
        self.path_temperatura2 = '//*[@id="main-content"]/div/div/div/div[1]/div[4]/div[2]/div/div[1]'
        self.path_temperatura_max_min2 = '//*[@id="main-content"]/div/div/div/div[1]/div[4]/div[2]/div/div[2]'
        self.path_nebulosidade2 = '//*[@id="main-content"]/div/div/div/div[1]/div[4]/div[1]/div/div[2]'
        self.path_umidade2 = '//*[@id="main-content"]/div/div/div/div[1]/div[4]/div[4]/div/div[1]'
        self.path_vento2 = '//*[@id="main-content"]/div/div/div/div[1]/div[4]/div[3]/div/div[1]'
        self.path_situacao_pesca1 = '//*[@id="main-content"]/div/div/div/div[1]/div[5]/div/h2/span'
        self.path_situacao_pesca2 = '//*[@id="main-content"]/div/div/div/div[1]/div[3]/div/h2/span'
        
        self.path_horario_por_sol = '//*[@id="main-content"]/div/div/div/div[1]/div[3]/div/div/table/tbody/tr[1]/td[7]'
        self.path_melhor_horario_pesca1 = '//*[@id="main-content"]/div/div/div/div[1]/div[5]/div/div/div[1]/div/h4'
        self.path_melhor_horario_pesca2 = '//*[@id="main-content"]/div/div/div/div[1]/div[5]/div/div/div[1]/div/h4[2]'
        self.path_por_sol = '//*[@id="main-content"]/div/div/div/div[1]/div[3]/div/div/table/tbody/tr[1]/td[7]'
        self.path_mare1 = '//*[@id="main-content"]/div/div/div/div[1]/div[3]/div/div/table/tbody/tr[1]/td[2]'
        self.path_mare2 = '//*[@id="main-content"]/div/div/div/div[1]/div[3]/div/div/table/tbody/tr[2]/td[3]'
        self.path_mare3 = '//*[@id="main-content"]/div/div/div/div[1]/div[3]/div/div/table/tbody/tr[1]/td[4]'
        self.path_mare4 = '//*[@id="main-content"]/div/div/div/div[1]/div[3]/div/div/table/tbody/tr[1]/td[5]'
        
        # path dos conteúdos do site de navios
        self.path_qtd_navios_porto = '//*[@id="generalInfo"]/div[2]/div/div/div/div/p[3]/b/a'
        self.path_qtd_navios_chegando = '//*[@id="generalInfo"]/div[2]/div/div/div/div/p[4]/b/a'
        
        # data de hoje
        self.data_hoje = self.get_dia_atual()
    
    # ordenação do discurso
    def ordenacao_discurso(self, dict_resultados):
        lista_ordenacao = ['intent_localizacao', 'intent_tempo', 'intent_temperatura', 'intent_umidade', 'intent_nebulosidade', 'intent_vento', 'intent_mar', 'intent_navios_porto', 'intent_navios_chegando']
        df_resultados = pd.DataFrame(dict_resultados, index=['index'])
        df_resultados = df_resultados[lista_ordenacao]
        dict_resultados = df_resultados.to_dict('records')[0]
        return dict_resultados

    def trata_diferenca(self, valor, valor_anterior):
        '''
        trata diferenca dos textos
        '''
        try:
            diferenca_porcentagem = round(((float(valor)/float(valor_anterior)) - 1), 2)
            if diferenca_porcentagem > 0:
                texto_diferenca = 'incremento'
            elif diferenca_porcentagem < 0:
                texto_diferenca = 'decremento'
            else:
                texto_diferenca = ''
            diferenca_porcentagem = str(abs(int(100 * diferenca_porcentagem))) + '%'
        except:
            texto_diferenca = ''
            diferenca_porcentagem = '' 
        return texto_diferenca, diferenca_porcentagem

    def estruturacao_texto(self, dict_resultados):
        return dict_resultados
    
        lista_paragrafo_1 = ['intent_localizacao', 'intent_tempo']
        lista_paragrafo_2 = ['intent_umidade', 'intent_vento', 'intent_mar']
        lista_paragrafo_3 = ['intent_navios_porto', 'intent_navios_chegando']

        # dict
        dict_1 = {key: dict_resultados[key] for key in lista_paragrafo_1}
        dict_2 = {key: dict_resultados[key] for key in lista_paragrafo_2}
        dict_3 = {key: dict_resultados[key] for key in lista_paragrafo_3}

        # lista de dicionários
        lista_dict_resultados = [dict_1, dict_2, dict_3]
        return lista_dict_resultados

    def get_element(self, dict_intents, intent, element):
        '''
        retorna valor do elemento do intent
        '''
        try:
            texto = dict_intents[intent]
            texto = texto.split(f'{element}="')[1].split('"')[0]
            try:
                texto = str(self.cast_float(texto)).replace('.',',')
                return texto
            except:
                return texto
        except:
            return ""
        
    # lexicalizacao
    def lexicalizacao(self, dict_resultados):
        
        # localizacao
        intent = 'intent_localizacao'
        data = self.get_element(dict_resultados, intent, 'Data')
        cidade = self.get_element(dict_resultados, intent, 'Cidade')
        uf = self.get_element(dict_resultados, intent, 'UF')
        
        # cidade
        if cidade in self.lista_cidades_no:
            em = 'no'
        else:
            em = 'em'
            
        # texto localizacao
        if (len(data) >= self.min and len(cidade) >= self.min and len(uf) >= self.min):
            texto_localizacao = f'Hoje, dia {data}, {em} {cidade} ({uf})'
        else:
            return ''
        
        # tempo
        intent = 'intent_tempo'
        condicao = self.get_element(dict_resultados, intent, 'Condição Tempo')
        horario_por_sol = self.get_element(dict_resultados, intent, 'Horario do por do sol')
        
        # texto tempo
        if (len(condicao) >= self.min and len(horario_por_sol) >= self.min):
            texto_tempo = f'a condição do tempo é de {condicao}, e o por do sol será às {horario_por_sol}.\n\n'
        else:
            return ''
        
        # mar
        intent = 'intent_mar'
        caso = 'altura do mar'
        contexto = 'Mar'
        valor = self.get_element(dict_resultados, intent, f'{contexto} Valor')
        valor_anterior = self.get_element(dict_resultados, intent, f'{contexto} Anterior')
        diferenca_porcentagem = self.get_element(dict_resultados, intent, f'{contexto} Diferenca porcentagem')
        texto_diferenca = self.get_element(dict_resultados, intent, f'{contexto} Texto diferenca')
        valor_max = self.get_element(dict_resultados, intent, f'{contexto} Máxima')
        valor_min = self.get_element(dict_resultados, intent, f'{contexto} Mínima')
        valor_med = self.get_element(dict_resultados, intent, f'{contexto} Média')
        qtd_dias_max = self.get_element(dict_resultados, intent, f'{contexto} Quantidade dias max')
        qtd_dias_min = self.get_element(dict_resultados, intent, f'{contexto} Quantidade dias min')
        tendencia = self.get_element(dict_resultados, intent, f'{contexto} Flag Tendência')
        outlier = self.get_element(dict_resultados, intent, f'{contexto} Flag Outlier')
        condicao_pesca = self.get_element(dict_resultados, intent, f'{contexto} Condição Pesca')
        melhor_horario_pescar = self.get_element(dict_resultados, intent, f'{contexto} Melhor horário para pescar')
        
        # texto mar
        if valor == '':
            texto_mar = ''
        else:
            valor = str(valor).replace('.',',')
            texto_mar = f'A {caso} esperada na cidade é de {valor}m'
            
            if outlier == '':
                texto_mar += '.'
            
            else:
                if outlier == '1':
                    texto_mar += f', e esse valor é um outlier superior.'
            
                elif outlier == '-1':
                    texto_mar += f', e esse valor é um outlier inferior.'
                    
            if qtd_dias_max != '' and qtd_dias_max !=0:
                texto_mar += f'Essa é a maior {caso} registrada nos últimos {qtd_dias_max} dias!'
            elif qtd_dias_min != '' and qtd_dias_min !=0:
                texto_mar += f'Essa é a maior {caso} registrada nos últimos {qtd_dias_min} dias!'
            
            if tendencia != '':
                texto_mar += f'A tendência da {caso} hoje é de {tendencia} em relação à média na última semana.'

            if texto_diferenca != '' and diferenca_porcentagem != '' and valor_anterior != '':
                valor_anterior = str(valor_anterior).replace('.',',')
                texto_mar += f'A {caso} teve um {texto_diferenca} de {diferenca_porcentagem} em relação ao dia anterior, que teve uma {caso} de {valor_anterior}m.'

            if valor_max != '' and valor_min != '' and valor_max > valor_min:
                texto_mar += f'As {caso}s máxima e mínima esperadas são de {valor_max}m e {valor_min}m, respectivamente.'
                
            if condicao_pesca != '' and melhor_horario_pescar != '':
                texto_mar += f'{condicao_pesca} O melhor horário para pescar é às {melhor_horario_pescar}.'
                
            elif condicao_pesca != '':
                texto_mar += f'{condicao_pesca}'
        
        # temperatura
        intent = 'intent_temperatura'
        caso = 'temperatura'
        contexto = 'Temperatura'
        valor = self.get_element(dict_resultados, intent, f'{contexto} Valor')
        valor_anterior = self.get_element(dict_resultados, intent, f'{contexto} Anterior')
        diferenca_porcentagem = self.get_element(dict_resultados, intent, f'{contexto} Diferenca porcentagem')
        texto_diferenca = self.get_element(dict_resultados, intent, f'{contexto} Texto diferenca')
        valor_max = self.get_element(dict_resultados, intent, f'{contexto} Máxima')
        valor_min = self.get_element(dict_resultados, intent, f'{contexto} Mínima')
        valor_med = self.get_element(dict_resultados, intent, f'{contexto} Média')
        qtd_dias_max = self.get_element(dict_resultados, intent, f'{contexto} Quantidade dias max')
        qtd_dias_min = self.get_element(dict_resultados, intent, f'{contexto} Quantidade dias min')
        tendencia = self.get_element(dict_resultados, intent, f'{contexto} Flag Tendência')
        outlier = self.get_element(dict_resultados, intent, f'{contexto} Flag Outlier')
        
        # texto temperatura
        if valor == '':
            texto_temperatura = ''
        else:
            valor = str(valor).replace('.',',')
            texto_temperatura = f'A {caso} esperada na cidade é de {valor}ºC'
            
            if outlier == '':
                texto_temperatura += '.'
            
            else:
                if outlier == '1':
                    texto_temperatura += f', e esse valor é um outlier superior.'
            
                elif outlier == '-1':
                    texto_temperatura += f', e esse valor é um outlier inferior.'
            
            if qtd_dias_max != '' and qtd_dias_max !=0:
                texto_temperatura += f'Essa é a maior {caso} registrada nos últimos {qtd_dias_max} dias!'
            elif qtd_dias_min != '' and qtd_dias_min !=0:
                texto_temperatura += f'Essa é a menor {caso} registrada nos últimos {qtd_dias_min} dias!'
                    
            if tendencia != '':
                texto_temperatura += f'A tendência da {caso} hoje é de {tendencia} em relação à média na última semana.'

            if texto_diferenca != '' and diferenca_porcentagem != '' and valor_anterior != '':
                valor_anterior = str(valor_anterior).replace('.',',')
                texto_temperatura += f'A {caso} teve um {texto_diferenca} de {diferenca_porcentagem} em relação ao dia anterior, que teve uma {caso} de {valor_anterior}ºC.'

            if valor_max != '' and valor_min != '' and valor_max > valor_min:
                texto_temperatura += f'As {caso}s máxima e mínima esperadas são de {valor_max}ºC e {valor_min}ºC, respectivamente.'   

        # umidade
        intent = 'intent_umidade'
        caso = 'umidade'
        contexto = 'Umidade'
        valor = self.get_element(dict_resultados, intent, f'{contexto} Valor')
        valor_anterior = self.get_element(dict_resultados, intent, f'{contexto} Anterior')
        diferenca_porcentagem = self.get_element(dict_resultados, intent, f'{contexto} Diferenca porcentagem')
        texto_diferenca = self.get_element(dict_resultados, intent, f'{contexto} Texto diferenca')
        valor_max = self.get_element(dict_resultados, intent, f'{contexto} Máxima')
        valor_min = self.get_element(dict_resultados, intent, f'{contexto} Mínima')
        valor_med = self.get_element(dict_resultados, intent, f'{contexto} Média')
        qtd_dias_max = self.get_element(dict_resultados, intent, f'{contexto} Quantidade dias max')
        qtd_dias_min = self.get_element(dict_resultados, intent, f'{contexto} Quantidade dias min')
        tendencia = self.get_element(dict_resultados, intent, f'{contexto} Flag Tendência')
        outlier = self.get_element(dict_resultados, intent, f'{contexto} Flag Outlier')

        # texto umidade
        if valor == '':
            texto_umidade = ''
        else:
            valor = str(valor).replace('.',',')
            texto_umidade = f'A {caso} esperada na cidade é de {valor}%'

            if outlier == '':
                texto_umidade += '.'

            else:
                if outlier == '1':
                    texto_umidade += f', e esse valor é um outlier superior.'

                elif outlier == '-1':
                    texto_umidade += f', e esse valor é um outlier inferior.'

            if qtd_dias_max != '' and qtd_dias_max !=0:
                texto_umidade += f'Essa é a maior {caso} registrada nos últimos {qtd_dias_max} dias!'
            elif qtd_dias_min != '' and qtd_dias_min !=0:
                texto_umidade += f'Essa é a maior {caso} registrada nos últimos {qtd_dias_min} dias!'

            if tendencia != '':
                texto_umidade += f'A tendência da {caso} hoje é de {tendencia} em relação à média na última semana.'

            if texto_diferenca != '' and diferenca_porcentagem != '' and valor_anterior != '':
                valor_anterior = str(valor_anterior).replace('.',',')
                texto_umidade += f'A {caso} teve um {texto_diferenca} de {diferenca_porcentagem} em relação ao dia anterior, que teve uma {caso} de {valor_anterior}%.'

            if valor_max != '' and valor_min != '' and valor_max > valor_min:
                texto_umidade += f'As {caso}s máxima e mínima esperadas são de {valor_max}% e {valor_min}%, respectivamente.'

        # nebulosidade
        intent = 'intent_nebulosidade'
        caso = 'nebulosidade'
        contexto = 'Nebulosidade'
        valor = self.get_element(dict_resultados, intent, f'{contexto} Valor')
        valor_anterior = self.get_element(dict_resultados, intent, f'{contexto} Anterior')
        diferenca_porcentagem = self.get_element(dict_resultados, intent, f'{contexto} Diferenca porcentagem')
        texto_diferenca = self.get_element(dict_resultados, intent, f'{contexto} Texto diferenca')
        valor_max = self.get_element(dict_resultados, intent, f'{contexto} Máxima')
        valor_min = self.get_element(dict_resultados, intent, f'{contexto} Mínima')
        valor_med = self.get_element(dict_resultados, intent, f'{contexto} Média')
        qtd_dias_max = self.get_element(dict_resultados, intent, f'{contexto} Quantidade dias max')
        qtd_dias_min = self.get_element(dict_resultados, intent, f'{contexto} Quantidade dias min')
        tendencia = self.get_element(dict_resultados, intent, f'{contexto} Flag Tendência')
        outlier = self.get_element(dict_resultados, intent, f'{contexto} Flag Outlier')

        # texto nebulosidade
        if valor == '':
            texto_nebulosidade = ''
        else:
            valor = str(valor).replace('.',',')
            texto_nebulosidade = f'A {caso} esperada na cidade é de {valor}%'

            if outlier == '':
                texto_nebulosidade += '.'

            else:
                if outlier == '1':
                    texto_nebulosidade += f', e esse valor é um outlier superior.'

                elif outlier == '-1':
                    texto_nebulosidade += f', e esse valor é um outlier inferior.'

            if qtd_dias_max != '' and qtd_dias_max !=0:
                texto_nebulosidade += f'Essa é a maior {caso} registrada nos últimos {qtd_dias_max} dias!'
            elif qtd_dias_min != '' and qtd_dias_min !=0:
                texto_nebulosidade += f'Essa é a maior {caso} registrada nos últimos {qtd_dias_min} dias!'

            if tendencia != '':
                texto_nebulosidade += f'A tendência da {caso} hoje é de {tendencia} em relação à média na última semana.'

            if texto_diferenca != '' and diferenca_porcentagem != '' and valor_anterior != '':
                valor_anterior = str(valor_anterior).replace('.',',')
                texto_nebulosidade += f'A {caso} teve um {texto_diferenca} de {diferenca_porcentagem} em relação ao dia anterior, que teve uma {caso} de {valor_anterior}%.'

            if valor_max != '' and valor_min != '' and valor_max > valor_min:
                texto_nebulosidade += f'As {caso}s máxima e mínima esperadas são de {valor_max}% e {valor_min}, respectivamente.'


        # velocidade do vento
        intent = 'intent_vento'
        caso = 'velocidade do vento'
        contexto = 'Vento'
        valor = self.get_element(dict_resultados, intent, f'{contexto} Valor')
        valor_anterior = self.get_element(dict_resultados, intent, f'{contexto} Anterior')
        diferenca_porcentagem = self.get_element(dict_resultados, intent, f'{contexto} Diferenca porcentagem')
        texto_diferenca = self.get_element(dict_resultados, intent, f'{contexto} Texto diferenca')
        valor_max = self.get_element(dict_resultados, intent, f'{contexto} Máxima')
        valor_min = self.get_element(dict_resultados, intent, f'{contexto} Mínima')
        valor_med = self.get_element(dict_resultados, intent, f'{contexto} Média')
        qtd_dias_max = self.get_element(dict_resultados, intent, f'{contexto} Quantidade dias max')
        qtd_dias_min = self.get_element(dict_resultados, intent, f'{contexto} Quantidade dias min')
        tendencia = self.get_element(dict_resultados, intent, f'{contexto} Flag Tendência')
        outlier = self.get_element(dict_resultados, intent, f'{contexto} Flag Outlier')

        # texto vento
        if valor == '':
            texto_vento = ''
        else:
            valor = str(valor).replace('.',',')
            texto_vento = f'A {caso} esperada na cidade é de {valor}km/h'

            if outlier == '':
                texto_vento += '.'

            else:
                if outlier == '1':
                    texto_vento += f', e esse valor é um outlier superior.'

                elif outlier == '-1':
                    texto_vento += f', e esse valor é um outlier inferior.'

            if qtd_dias_max != '' and qtd_dias_max !=0:
                texto_vento += f'Essa é a maior {caso} registrada nos últimos {qtd_dias_max} dias!'
            elif qtd_dias_min != '' and qtd_dias_min !=0:
                texto_vento += f'Essa é a menor {caso} registrada nos últimos {qtd_dias_min} dias!'

            if tendencia != '':
                texto_vento += f'A tendência da {caso} hoje é de {tendencia} em relação à média na última semana.'

            if texto_diferenca != '' and diferenca_porcentagem != '' and valor_anterior != '':
                valor_anterior = str(valor_anterior).replace('.',',')
                texto_vento += f'A {caso} teve um {texto_diferenca} de {diferenca_porcentagem} em relação ao dia anterior, que teve uma {caso} de {valor_anterior}km/h.'

            if valor_max != '' and valor_min != '' and valor_max > valor_min:
                texto_vento += f'As {caso}s máxima e mínima esperadas são de {valor_max}km/h e {valor_min}km/h, respectivamente.'


        # qtd de navios no porto
        intent = 'intent_navios_porto'
        caso = 'quantidade de navios atualmente no porto'
        contexto = 'Navios porto'
        valor = self.get_element(dict_resultados, intent, f'{contexto} Valor')
        valor_anterior = self.get_element(dict_resultados, intent, f'{contexto} Anterior')
        diferenca_porcentagem = self.get_element(dict_resultados, intent, f'{contexto} Diferenca porcentagem')
        texto_diferenca = self.get_element(dict_resultados, intent, f'{contexto} Texto diferenca')
        valor_max = self.get_element(dict_resultados, intent, f'{contexto} Máxima')
        valor_min = self.get_element(dict_resultados, intent, f'{contexto} Mínima')
        valor_med = self.get_element(dict_resultados, intent, f'{contexto} Média')
        qtd_dias_max = self.get_element(dict_resultados, intent, f'{contexto} Quantidade dias max')
        qtd_dias_min = self.get_element(dict_resultados, intent, f'{contexto} Quantidade dias min')
        tendencia = self.get_element(dict_resultados, intent, f'{contexto} Flag Tendência')
        outlier = self.get_element(dict_resultados, intent, f'{contexto} Flag Outlier')

        # texto quantidade de navios no porto
        if valor == '':
            texto_navios_atualmente_porto = ''
        else:
            valor = str(valor).replace('.',',')
            texto_navios_atualmente_porto = f'A {caso} é de {valor}'

        if outlier == '':
            texto_navios_atualmente_porto += '.'

        else:
            if outlier == '1':
                texto_navios_atualmente_porto += f', e esse valor é um outlier superior.'

            elif outlier == '-1':
                texto_navios_atualmente_porto += f', e esse valor é um outlier inferior.'

        if qtd_dias_max != '' and qtd_dias_max !=0:
            texto_navios_atualmente_porto += f'Essa é a maior {caso} registrada nos últimos {qtd_dias_max} dias!'
        elif qtd_dias_min != '' and qtd_dias_min !=0:
            texto_navios_atualmente_porto += f'Essa é a menor {caso} registrada nos últimos {qtd_dias_min} dias!'

        if tendencia != '':
            texto_navios_atualmente_porto += f'A tendência da {caso} hoje é de {tendencia} em relação à média na última semana.'

        if texto_diferenca != '' and diferenca_porcentagem != '' and valor_anterior != '':
            valor_anterior = str(valor_anterior).replace('.',',')
            texto_navios_atualmente_porto += f'A {caso} teve um {texto_diferenca} de {diferenca_porcentagem} em relação ao dia anterior, que teve uma {caso} de {valor_anterior}.'

        if valor_max != '' and valor_min != '' and valor_max > valor_min:
            texto_navios_atualmente_porto += f'As {caso}s máxima e mínima esperadas são de {valor_max} e {valor_min}, respectivamente.'


        # qtd de navios chegando no porto nos próximos dias
        intent = 'intent_navios_chegando'
        caso = 'quantidade de navios chegando no porto'
        contexto = 'Navios chegando'
        valor = self.get_element(dict_resultados, intent, f'{contexto} Valor')
        valor_anterior = self.get_element(dict_resultados, intent, f'{contexto} Anterior')
        diferenca_porcentagem = self.get_element(dict_resultados, intent, f'{contexto} Diferenca porcentagem')
        texto_diferenca = self.get_element(dict_resultados, intent, f'{contexto} Texto diferenca')
        valor_max = self.get_element(dict_resultados, intent, f'{contexto} Máxima')
        valor_min = self.get_element(dict_resultados, intent, f'{contexto} Mínima')
        valor_med = self.get_element(dict_resultados, intent, f'{contexto} Média')
        qtd_dias_max = self.get_element(dict_resultados, intent, f'{contexto} Quantidade dias max')
        qtd_dias_min = self.get_element(dict_resultados, intent, f'{contexto} Quantidade dias min')
        tendencia = self.get_element(dict_resultados, intent, f'{contexto} Flag Tendência')
        outlier = self.get_element(dict_resultados, intent, f'{contexto} Flag Outlier')

        # texto quantidade de navios no porto
        if valor == '':
            texto_navios_chegando_porto = ''
        else:
            valor = str(valor).replace('.',',')
            texto_navios_chegando_porto = f'A {caso} é de {valor}'

        if outlier == '':
            texto_navios_chegando_porto += '.'

        else:
            if outlier == '1':
                texto_navios_chegando_porto += f', e esse valor é um outlier superior.'

            elif outlier == '-1':
                texto_navios_chegando_porto += f', e esse valor é um outlier inferior.'

        if qtd_dias_max != '' and qtd_dias_max !=0:
            texto_navios_chegando_porto += f'Essa é a maior {caso} registrada nos últimos {qtd_dias_max} dias!'
        elif qtd_dias_min != '' and qtd_dias_min !=0:
            texto_navios_chegando_porto += f'Essa é a menor {caso} registrada nos últimos {qtd_dias_min} dias!'

        if tendencia != '':
            texto_navios_chegando_porto += f'A tendência da {caso} hoje é de {tendencia} em relação à média na última semana.'

        if texto_diferenca != '' and diferenca_porcentagem != '' and valor_anterior != '':
            valor_anterior = str(valor_anterior).replace('.',',')
            texto_navios_chegando_porto += f'A {caso} teve um {texto_diferenca} de {diferenca_porcentagem} em relação ao dia anterior, que teve uma {caso} de {valor_anterior}.'

        if valor_max != '' and valor_min != '' and valor_max > valor_min:
            texto_navios_chegando_porto += f'As {caso}s máxima e mínima esperadas são de {valor_max} e {valor_min}, respectivamente.'

        # texto final lexicalizado
        texto_final = f'{texto_localizacao} {texto_tempo}\n\n{texto_temperatura}\n{texto_mar}\n'
        texto_final += f'{texto_umidade}\n{texto_nebulosidade}\n{texto_vento}\n'
        texto_final += f'{texto_navios_atualmente_porto}\n{texto_navios_chegando_porto}'
        texto_final = re.sub(r'(?<=[.!?])(?=[^\s])', r' ', texto_final)
        return texto_final
    
    def rreplace(self, text, old, new, occurrence):
        '''
        substitui ocorrência
        '''
        lista_resultados = text.rsplit(old, occurrence)
        return new.join(lista_resultados)

    # gera expressoes de referencia
    def gera_expressoes_referencia(self, texto):
        list_partitions = ['na cidade']
        for partition in list_partitions:
            texto = self.rreplace(texto, partition, '', texto.count(partition) - 1)
        return texto

    # realização superficial
    def realizacao_superficial(self, texto):
        # substitui bom dia, caso se aplique
        hora_atual = int(datetime.datetime.now().hour)
        
        # incio
        lista_possibilidades_inicio = ['',
                                       '',
                                       'Olá!',
                                       'Olá, tudo bem por aqui?',
                                       'Bom dia a todos, tudo bem com vocês?',
                                       'Olá, tudo bem com vocês?',
                                       'E vamos para mais uma notícia!',
                                       'Nova notícia saindo do forno!',
                                       'Notícia nova saindo do forno!',
                                       'Olá, como vocês estão?',
                                       'Estou de volta para mais uma notícia!']

        # fim
        lista_possibilidades_fim = ['',
                                    '',
                                    'Desejo um excelente dia para todos.',
                                    'Desejo um ótimo dia para todos.',
                                    'Um bom dia para todos!',
                                    'Um excelente dia para todos vocês!']
        
        # escolha
        inicio = random.choice(lista_possibilidades_inicio)
        fim = random.choice(lista_possibilidades_fim)
        texto = f'{inicio} {texto} {fim}'
        
        # dia
        if hora_atual <= 11:
            pass
          
        # noite
        elif hora_atual >= 19:
            texto = texto.replace('Bom dia', 'Boa noite')
          
        # tarde
        else:
            texto = texto.replace('Bom dia', 'Boa tarde')
        
        # mapeia emoticons
        for chave, valor in self.dict_map_emoticons.items():
            try:
                if chave in texto:
                    texto = texto.replace(chave, f'{chave} {valor}')
            except:
                pass
            
        # fim do texto
        return f"{texto}\n\n\n#AmazôniaAzul {self.twitter_api.dict_map_emoji['oceano']}\n#redebotsdobem {self.twitter_api.dict_map_emoji['satelite']}"
    
        # retorna resultados
        return texto     
                
    def cast_float(self, valor):
        '''
        cast float
        '''
        try:
            valor = valor.replace(',', '.')
            valor = float(valor)
            if valor % 1 == 0:
                return int(valor)
            else:
                return valor

        except Exception as e:
            return valor
        
    def get_dia_atual(self):
        '''
        data de hoje
        '''
        # data de hoje
        dia = date.today().strftime("%d")
        mes = self.dict_map_mes[int(date.today().strftime("%m"))]
        ano = date.today().strftime("%Y")
        return f"{dia} de {mes} de {ano}"

    
    def trata_mare(self, elemento):
        '''
        trata maré
        '''
        horario = (elemento.split('m')[0] + 'm').strip()
        altura = float(elemento.split(' ')[1].strip())
        simbolo = elemento.split(' ')[0][-1].strip()
        
        if simbolo == self.icone_up:
            simbolo = 'up'
        else:
            simbolo = 'down'
        return (horario, altura, simbolo)
        
    
    def trata_texto(file_name):
        '''
        Trata texto
        '''
        with open(file_name, 'r') as file :
             filedata = file.read()

        # Replace the target string
        filedata = filedata.replace('\\', '').replace('\"', '')

        # Write the file out again
        with open(file_name, 'w') as file:
            file.write(filedata)

    def get_analisador_lexico(self, palavra, numero, genero):
        '''
        Retorna início do texto para publicação
        '''
        return (self.dict_analisador_lexico[f"{palavra}|{numero}|{genero}"])
        
        
    def count_dias(self, data_hoje, lista_datas):
        '''
        retorna qtd de dias consecutivos
        '''
        qtd_dias = 0
        data = data_hoje
        while (True):
            data = data - datetime.timedelta(1)
            if data.strftime("%d/%m/%Y") in lista_datas:
                qtd_dias+=1
            else:
                break

        return qtd_dias


    def count_valor(self, valor_hoje, lista_valores, tipo_operacao):
        '''
        retorna qtd de dias onde o valor satisfaz determinada condição
        '''
        qtd_dias = 0

        try:
            if tipo_operacao == 'maior':
                for valor in lista_valores:
                    if (valor_hoje > valor):
                        qtd_dias +=1
                    else:
                        break

            elif tipo_operacao == 'menor':
                for valor in lista_valores:
                    if (valor_hoje < valor):
                        qtd_dias +=1
                    else:
                        break

            else:
                return 0
        except Exception as e:
            print (f'Erro count_valor: {valor_hoje} | {e}')

        # compara com mínimo
        if (qtd_dias < self.qtd_min_dias_consecutivos):
            return 0

        # retorna qtd de dias
        return qtd_dias
    
    
    def gera_df_navios(self):
        '''
        Gera resultados
        '''
    
        lista_infos = []

        # itera cidades
        count = 0
        for index, row in self.df_portos.iterrows():
            
            try:
                cidade = row['Cidade']
                url = row['URL']
                cidade_map = row['Map']
               
                # entra na url 2
                driver = webdriver.Chrome(self.path_to_chromedriver, options=self.chromeOptions)
                driver.get(url)
                time.sleep(8)
                    
                # quantidade de navios no porto e chegando
                qtd_navios_porto = int(driver.find_element_by_xpath(self.path_qtd_navios_porto).text)
                qtd_navios_chegando = int(driver.find_element_by_xpath(self.path_qtd_navios_chegando).text)
                
                # verifica outliers
                if (qtd_navios_porto > self.maior_qtd_navios or qtd_navios_chegando > self.maior_qtd_navios or\
                    qtd_navios_porto < self.menor_qtd_navios or qtd_navios_chegando < self.menor_qtd_navios):
                    continue
                
                # salva lista
                lista_infos.append([cidade_map,
                                    qtd_navios_porto,
                                    qtd_navios_chegando])
                driver.close()
                
                # break para debug
                count += 1
                if count >= self.flag_max:
                    break

            # erro de execução
            except Exception as e:
                print (f'1. Erro na cidade {cidade}! {url} | {e}')
                driver.close()
                continue
                                                  
        # cria o dataframe
        try:
            df_infos = pd.DataFrame(lista_infos,
                                    columns=self.lista_colunas_navios)

            # transforma tudo em string
            for column in df_infos.columns.tolist():
                df_infos[column] = df_infos[column].astype(str)

            # retorna resultados
            return df_infos
        
        # erro de execução
        except Exception as e:
            print (f'2. Erro no fim do processo! {e}')
            sys.exit(0)
            
            
    def gera_df_tempo(self):
        '''
        Gera resultados dos climas
        '''
    
        lista_infos = []

        # itera cidades
        count = 0
        for index, row in self.df_cidades.iterrows():
            
            try:

                cidade = row['Cidade']
                uf = row['UF']
                valor = row['Tabua_mares']

                # cria urls
                url_dia = f"{self.url_tabua_mares}/{valor}"
                
                # entra na url
                driver = webdriver.Chrome(self.path_to_chromedriver, options=self.chromeOptions)
                driver.get(url_dia)
                time.sleep(2)

                # leitura do conteúdo
                try:
                    tempo = driver.find_element_by_xpath(self.path_tempo1).text
                except:
                    try:
                        tempo = driver.find_element_by_xpath(self.path_tempo2).text
                    except Exception as e:
                        print (f'3. Erro na cidade {cidade}! {e}')
                        continue
                    
                try:
                    temperatura = int((driver.find_element_by_xpath(self.path_temperatura1).text).split('°C')[0])
                except:
                    try:
                        temperatura =  int((driver.find_element_by_xpath(self.path_temperatura2).text).split('°C')[0])
                    except Exception as e:
                        print (f'4. Erro na cidade {cidade}! {e}')
                        continue
                    
                          
                try:
                    temperatura_max_min = driver.find_element_by_xpath(self.path_temperatura_max_min1).text
                except:
                    try:
                        temperatura_max_min = driver.find_element_by_xpath(self.path_temperatura_max_min2).text
                    except Exception as e:
                        print (f'5. Erro na cidade {cidade}! {e}')
                        continue
                    
                try:
                    nebulosidade = int(driver.find_element_by_xpath(self.path_nebulosidade1).text.split('Cloud cover ')[1].split('%')[0])
                except:
                    try:
                        nebulosidade = int(driver.find_element_by_xpath(self.path_nebulosidade2).text.split('Cloud cover ')[1].split('%')[0])
                    except Exception as e:
                        print (f'6. Erro na cidade {cidade}! {e}')
                        continue
                    
                
                try:
                    umidade = int(driver.find_element_by_xpath(self.path_umidade1).text.split('%')[0])
                except:
                    try:
                        umidade = int(driver.find_element_by_xpath(self.path_umidade2).text.split('%')[0])
                    except Exception as e:
                        print (f'7. Erro na cidade {cidade}! {e}')
                        continue
                     
                    
                try:
                    vento = int(driver.find_element_by_xpath(self.path_vento1).text.split(' ')[0])
                except:
                    try:
                        vento = int(driver.find_element_by_xpath(self.path_vento2).text.split(' ')[0])
                    except Exception as e:
                        print (f'8. Erro na cidade {cidade}! {e}')
                        continue
                    
                try:
                     pesca = driver.find_element_by_xpath(self.path_situacao_pesca1).text
                except:
                    try:
                        pesca = driver.find_element_by_xpath(self.path_situacao_pesca2).text
                    except Exception as e:
                        print (f'9. Erro na cidade {cidade}! {e}')
                        continue
                    
    
                # melhor horário de pesca
                try:
                    melhor_horario_pesca = driver.find_element_by_xpath(self.path_melhor_horario_pesca2).text.split('From ')[1].replace(' to ', ' - ')
                except:
                    try:
                        melhor_horario_pesca = driver.find_element_by_xpath(self.path_melhor_horario_pesca1).text.split('From ')[1].replace(' to ', ' - ')
                    except Exception as e:
                        melhor_horario_pesca = ''
                
                # trata melhor horário para pescar
                if melhor_horario_pesca != '':
                    try:
                        horario1, horario2 = melhor_horario_pesca.split("-")
                        horario1 = horario1.strip()
                        horario2 = horario2.strip()

                        # horario1
                        if "am" in horario1:
                            horario1 = horario1.split("am")[0]
                        elif "pm" in horario1:
                            horario1 = horario1.split("pm")[0]
                            hora, minuto = horario1.split(":")
                            hora = str(int(hora) + 12)
                            if int(hora) == 12:
                                hora = "0"
                            horario1 = f"{hora}:{minuto}"

                        # horario2
                        if "am" in horario2:
                            horario2 = horario2.split("am")[0]
                        elif "pm" in horario2:
                            horario2 = horario2.split("pm")[0]
                            hora, minuto = horario2.split(":")
                            hora = str(int(hora) + 12)
                            if int(hora) == 12:
                                hora = "0"
                            horario2 = f"{hora}:{minuto}"

                        melhor_horario_pesca = f"{horario1} - {horario2}"

                    except:
                        pass
                        
                # por do sol
                horario_por_sol = str(driver.find_element_by_xpath(self.path_horario_por_sol).text).split(' ')[1].strip()
                if "am" in horario_por_sol:
                    horario_por_sol = horario_por_sol.split("am")[0]
                elif "pm" in horario_por_sol:
                    horario_por_sol = horario_por_sol.split("pm")[0]
                    hora, minuto = horario_por_sol.split(":")
                    hora = str(int(hora) + 12)
                    horario_por_sol = f"{hora}:{minuto}"
                
                # marés
                try:
                    horario_mare_1, altura_mare_1, simbolo_mare_1 = self.trata_mare(driver.find_element_by_xpath(self.path_mare1).text)
                except:
                    horario_mare_1 = ""
                    altura_mare_1 = -1
                    simbolo_mare_1 = ""
                    
                try:
                    horario_mare_2, altura_mare_2, simbolo_mare_2 = self.trata_mare(driver.find_element_by_xpath(self.path_mare2).text)
                except:
                    horario_mare_2 = ""
                    altura_mare_2 = -1
                    simbolo_mare_2 = ""
                    
                try:
                    horario_mare_3, altura_mare_3, simbolo_mare_3 = self.trata_mare(driver.find_element_by_xpath(self.path_mare3).text)
                except:
                    horario_mare_3 = ""
                    altura_mare_3 = -1
                    simbolo_mare_3 = ""
                
                try:
                    horario_mare_4, altura_mare_4, simbolo_mare_4 = self.trata_mare(driver.find_element_by_xpath(self.path_mare4).text)
                
                except:
                    horario_mare_4 = ""
                    altura_mare_4 = -1
                    simbolo_mare_4 = ""
                    
                # altura da maior onda
                altura_maior_onda = str(max(altura_mare_1, altura_mare_2, altura_mare_3, altura_mare_4))

                # tratamento dos campos
                altura_maior_onda = str(altura_maior_onda).replace('.',',')
                altura_mare_1 = str(altura_mare_1).replace('.',',')
                altura_mare_2 = str(altura_mare_2).replace('.',',')
                altura_mare_3 = str(altura_mare_3).replace('.',',')
                altura_mare_4 = str(altura_mare_4).replace('.',',')
                
                # texto onda
                if (simbolo_mare_1 == ""):
                    continue
                
                elif (simbolo_mare_1 == "up" and simbolo_mare_2 == ""):
                     texto_onda = f"Horário de maré alta: {horario_mare_1} ({altura_mare_1}m)"
                        
                elif (simbolo_mare_1 == "down" and simbolo_mare_2 == ""):
                     texto_onda = f"Horário de maré baixa: {horario_mare_1} ({altura_mare_1}m)"
                              
                elif (simbolo_mare_1 == 'up' and simbolo_mare_4 != ""):
                    
                    texto_onda = f"Horários de maré alta: {horario_mare_1} ({altura_mare_1}m) e {horario_mare_3} ({altura_mare_3}m).\nHorários de maré baixa: {horario_mare_2} ({altura_mare_2}m) e {horario_mare_4} ({altura_mare_4}m)"
                
                elif (simbolo_mare_1 == 'up' and simbolo_mare_4 == ""):
                    texto_onda = f"Horários de maré alta: {horario_mare_1} ({altura_mare_1}m) e {horario_mare_3} ({altura_mare_3}m).\nHorário de maré baixa: {horario_mare_2} ({altura_mare_2}m)"
                    
                elif (simbolo_mare_1 == 'down' and simbolo_mare_4 != ""):
                    
                    texto_onda = f"Horário de maré alta: {horario_mare_2} ({altura_mare_2}m) e {horario_mare_4} ({altura_mare_4}m).\nHorários de maré baixa: {horario_mare_1} ({altura_mare_1}m) e {horario_mare_3} ({altura_mare_3}m)"
                
                elif (simbolo_mare_1 == 'down' and simbolo_mare_4 == ""):
                    texto_onda = f"Horário de maré alta: {horario_mare_2} ({altura_mare_2}m).\nHorários de maré baixa: {horario_mare_1} ({altura_mare_1}m) e {horario_mare_3} ({altura_mare_3}m)"
                    
                else:
                    continue
                  
                # temperatura max
                temperatura_max = int(temperatura_max_min.split('Min ')[1].split('°')[0])
                
                # temperatura min
                temperatura_min = int(temperatura_max_min.split('Max ')[1].split('°')[0])
                
                # validação conceitual de temperaturas
                maior_valor_temperatura = max([temperatura, temperatura_max, temperatura_min])
                menor_valor_temperatura = min([temperatura, temperatura_max, temperatura_min])                         

                # se estiver fora da range, ignora a cidade e continua o processo
                if (maior_valor_temperatura > self.maior_temperatura_possivel or menor_valor_temperatura < self.menor_temperatura_possivel):
                    continue
                    
                # mapeia tempo
                if tempo in self.dict_map_clima.keys():
                    try:
                        tempo = self.dict_map_clima[tempo]
                    except:
                        tempo = self.filler_tempo
                else:
                    tempo = self.filler_tempo
                     
                # mapeia pesca
                if pesca in self.dict_map_pesca.keys():
                    try:
                        pesca = self.dict_map_pesca[pesca]
                    except:
                        pesca = ''
                else:
                    pesca = ''
                
                # salva lista
                lista_infos.append([cidade,
                                    uf,
                                    tempo,
                                    temperatura,
                                    temperatura_max,
                                    temperatura_min,
                                    nebulosidade,
                                    umidade,
                                    vento,
                                    horario_por_sol,
                                    pesca,
                                    melhor_horario_pesca,
                                    altura_maior_onda,
                                    texto_onda])
                
                # break para debug
                count += 1
                if count >= self.flag_max:
                    break

            # erro de execução
            except Exception as e:
                print (f'10. Erro na cidade {cidade}! {e}')
                continue
                                                  
        # cria o dataframe
        try:
            df_infos = pd.DataFrame(lista_infos,
                                    columns=self.lista_colunas_tempo)

            # tratamentos adicionais
            df_infos['temperatura_max'] = df_infos[["temperatura", "temperatura_max", "temperatura_min"]].max(axis=1)
            df_infos['temperatura_min'] = df_infos[["temperatura", "temperatura_max", "temperatura_min"]].min(axis=1)

            # transforma tudo em string
            for column in df_infos.columns.tolist():
                df_infos[column] = df_infos[column].astype(str)

            # retorna resultados
            return df_infos
        
        # erro de execução
        except Exception as e:
            print (f'11. Erro no fim do processo! {e}')
            sys.exit(0)

     
    def monta_intent(self, texto, intent, atributo, valor):
        if valor == 0 or valor == '':
            return texto
        
        if len(texto) >=1 and texto[-1] == ')':
            texto = texto[:-1]
        if intent not in texto:
            texto = texto + intent + '(' + texto + f'''{atributo}="{valor}")'''
        else:
            texto = texto + f''',{atributo}="{valor}")'''
        
        # texto
        return texto
        
    
    def seleciona_conteudo_publicar(self, df_resultados):
        '''
        Seleciona conteúdo para publicar, de acordo com a estratégia implementada
        '''
        return df_resultados

        try:
            # estratéga de seleção de conteúdo
            # dados prioritários
            cidades_priorizadas = ['Santos',
                                   'Rio de Janeiro',
                                   'Paranagua',
                                   'Porto de Galinhas - Ipojuca',
                                   'Rio Grande',
                                   'Suape',
                                   'Itajai']
            
            # max e min temperatura
            df_sort = df_resultados.sort_values(by=['temperatura'], ascending=False)
            df_max_temperatura = df_sort.iloc[0]
            df_min_temperatura = df_sort.iloc[-1]
            
            # caso seja vazio
            if (df_max_temperatura['cidade'] == df_min_temperatura['cidade']):
                df_min_temperatura = pd.DataFrame()

            # df priorizado
            df_priorizado = df_resultados.loc[df_resultados['cidade'].isin(cidades_priorizadas)]
            
            # outros
            df_outros = df_resultados.loc[~df_resultados['cidade'].isin(cidades_priorizadas)]
            
            # priorização
            if (len(df_priorizado) == 0):
                return df_outros
            
            # priorização
            elif (len(df_outros) == 0):
                return df_priorizado
            
            # df selecionado
            max_observacoes = min(len(df_outros), self.qtd_cidades_selecionadas)
            df_selecionados = df_outros.sample(max_observacoes)
            df_selecionados = pd.concat([df_priorizado, df_max_temperatura, df_min_temperatura, df_selecionados])
            df_selecionados = df_selecionados.drop_duplicates(subset=['cidade'])
            
            # flag
            cidade_tmp_max = df_max_temperatura['cidade']
            df_selecionados['flag_temperatura_maxima'] = [1 if element == cidade_tmp_max else 0 for element in df_selecionados['cidade'].values.tolist()]
            
            if len(df_min_temperatura) > 0:
                cidade_tmp_min = df_min_temperatura['cidade']
                df_selecionados['flag_temperatura_minima'] = [1 if element == cidade_tmp_min else 0 for element in df_selecionados['cidade'].values.tolist()]

            # retorna resultados selecionados
            return df_selecionados
        except Exception as e:
            print (f'12.1. Erro! {e}')
            sys.exit(0)
        
    def expressao_referencia(self, texto):
        
        # lista de expressoes
        dict_expressoes = {'cidade':'cidade_luz'}
        
        # lista de expressoes
        for expressao, lista_substituicao in lista_expressoes:
            substituicao = random.choice(lista_substituicao)
            texto = texto.partition(expressao)
            texto = texto[0] + texto[1] + texto[2].replace(expressao, substituicao)
            
        # retorna texto
        return texto
        
    def selecao_conteudo(self, df_linha):
        '''
        retorna intent com base no conteúdo
        '''

        # linha
        df_linha = df_linha.apply(self.cast_float)

        # dict
        infos = df_linha.transpose().to_dict()
            
        # check de qualidade do texto
        if (infos['data'] == '' or infos['cidade'] == '' or infos['uf'] == ''):
            return 0

        # selecao de conteúdo
        try:
            # Localizacao
            intent = 'Localizacao'
            intent_localizacao = ''
            intent_localizacao = self.monta_intent(intent_localizacao, intent, "Data", infos['data'])
            intent_localizacao = self.monta_intent(intent_localizacao, intent, "Cidade", infos['cidade'])
            intent_localizacao = self.monta_intent(intent_localizacao, intent, "UF", infos['uf'])
                     
            # Tempo
            intent = 'Tempo'
            intent_tempo = ''
            intent_tempo = self.monta_intent(intent_tempo, intent, "Condição Tempo", infos['tempo'])
            intent_tempo = self.monta_intent(intent_tempo, intent, "Horario do por do sol", infos['horario_por_sol'])
            
            # Temperatura
            intent = 'Temperatura'
            intent_temperatura = ''
            intent_temperatura = self.monta_intent(intent_temperatura, intent, "Temperatura Valor", infos['temperatura'])
            intent_temperatura = self.monta_intent(intent_temperatura, intent, "Temperatura Anterior", infos['temperatura_anterior'])
            intent_temperatura = self.monta_intent(intent_temperatura, intent, "Temperatura Máxima", infos['temperatura_max'])
            intent_temperatura = self.monta_intent(intent_temperatura, intent, "Temperatura Mínima", infos['temperatura_min'])
            intent_temperatura = self.monta_intent(intent_temperatura, intent, "Temperatura Média", infos['temperatura_media'])
            intent_temperatura = self.monta_intent(intent_temperatura, intent, "Temperatura Quantidade dias max", infos['temperatura_qtd_dias_max'])
            intent_temperatura = self.monta_intent(intent_temperatura, intent, "Temperatura Quantidade dias min", infos['temperatura_qtd_dias_min'])
            intent_temperatura = self.monta_intent(intent_temperatura, intent, "Temperatura Flag Tendência", infos['temperatura_flag_tendencia'])
            intent_temperatura = self.monta_intent(intent_temperatura, intent, "Temperatura Diferenca porcentagem", infos['temperatura_diferenca_porcentagem'])
            intent_temperatura = self.monta_intent(intent_temperatura, intent, 'Temperatura Texto diferenca', infos['temperatura_texto_diferenca'])
            intent_temperatura = self.monta_intent(intent_temperatura, intent, "Temperatura Flag Outlier", infos['temperatura_flag_outlier'])
            
            # Umidade
            intent = 'Umidade'
            intent_umidade = ''
            intent_umidade = self.monta_intent(intent_umidade, intent, "Umidade Valor", infos['umidade'])
            intent_umidade = self.monta_intent(intent_umidade, intent, "Umidade Anterior", infos['umidade_anterior'])
            intent_umidade = self.monta_intent(intent_umidade, intent, "Umidade Média", infos['umidade_media'])
            intent_umidade = self.monta_intent(intent_umidade, intent, "Umidade Quantidade dias max", infos['umidade_qtd_dias_max'])
            intent_umidade = self.monta_intent(intent_umidade, intent, "Umidade Quantidade dias min", infos['umidade_qtd_dias_min'])
            intent_umidade = self.monta_intent(intent_umidade, intent, "Umidade Flag Tendência", infos['umidade_flag_tendencia'])
            intent_umidade = self.monta_intent(intent_umidade, intent, "Umidade Diferenca porcentagem", infos['umidade_diferenca_porcentagem'])
            intent_umidade = self.monta_intent(intent_umidade, intent, 'Umidade Texto diferenca', infos['umidade_texto_diferenca'])
            intent_umidade = self.monta_intent(intent_umidade, intent, "Umidade Flag Outlier", infos['umidade_flag_outlier'])
            
            # Nebulosidade
            intent = 'Nebulosidade'
            intent_nebulosidade = ''
            intent_nebulosidade = self.monta_intent(intent_nebulosidade, intent, "Nebulosidade Valor", infos['nebulosidade'])
            intent_nebulosidade = self.monta_intent(intent_nebulosidade, intent, "Nebulosidade Anterior", infos['nebulosidade_anterior'])
            intent_nebulosidade = self.monta_intent(intent_nebulosidade, intent, "Nebulosidade Média", infos['nebulosidade_media'])
            intent_nebulosidade = self.monta_intent(intent_nebulosidade, intent, "Nebulosidade Quantidade dias max", infos['nebulosidade_qtd_dias_max'])
            intent_nebulosidade = self.monta_intent(intent_nebulosidade, intent, "Nebulosidade Quantidade dias min", infos['nebulosidade_qtd_dias_min'])
            intent_nebulosidade = self.monta_intent(intent_nebulosidade, intent, "Nebulosidade Flag Tendência", infos['nebulosidade_flag_tendencia'])
            intent_nebulosidade = self.monta_intent(intent_nebulosidade, intent, "Nebulosidade Diferenca porcentagem", infos['nebulosidade_diferenca_porcentagem'])
            intent_nebulosidade = self.monta_intent(intent_nebulosidade, intent, 'Nebulosidade Texto diferenca', infos['nebulosidade_texto_diferenca'])
            intent_nebulosidade = self.monta_intent(intent_nebulosidade, intent, "Nebulosidade Flag Outlier", infos['nebulosidade_flag_outlier'])
            
            # Vento
            intent = 'Vento'
            intent_vento = ''
            intent_vento = self.monta_intent(intent_vento, intent, "Vento Valor", infos['vento'])
            intent_vento = self.monta_intent(intent_vento, intent, "Vento Média", infos['vento_media'])
            intent_vento = self.monta_intent(intent_vento, intent, "Vento Anterior", infos['vento_anterior'])
            intent_vento = self.monta_intent(intent_vento, intent, "Vento Quantidade dias max", infos['vento_qtd_dias_max'])
            intent_vento = self.monta_intent(intent_vento, intent, "Vento Quantidade dias min", infos['vento_qtd_dias_min'])
            intent_vento = self.monta_intent(intent_vento, intent, "Vento Flag Tendência", infos['vento_flag_tendencia'])
            intent_vento = self.monta_intent(intent_vento, intent, "Vento Diferenca porcentagem", infos['vento_diferenca_porcentagem'])
            intent_vento = self.monta_intent(intent_vento, intent, 'Vento Texto diferenca', infos['vento_texto_diferenca'])
            intent_vento = self.monta_intent(intent_vento, intent, "Vento Flag Outlier", infos['vento_flag_outlier'])
            
            # Mar
            intent = 'Mar'
            intent_mar = ''
            
            if infos['pesca'] == "Excelente" and infos['altura_maior_onda'] != '':
                intent_mar = self.monta_intent(intent_mar, intent, 'Condição Pesca', "Hoje é um excelente dia para pescar.")
                intent_mar = self.monta_intent(intent_mar, intent, 'Melhor horário para pescar', infos['melhor_horario_pesca'])
                
            elif infos['pesca'] == "Bom" and infos['altura_maior_onda'] != '':
                intent_mar = self.monta_intent(intent_mar, intent, 'Condição Pesca', "Hoje é um bom dia para pescar.")
                intent_mar = self.monta_intent(intent_mar, intent, 'Melhor horário para pescar', infos['melhor_horario_pesca'])
                
            elif (infos['pesca'] not in ["Excelente", "Bom"] and infos['altura_maior_onda'] != '' and float(str(infos['altura_maior_onda']).replace(",",".")) > self.altura_mare_ruim):
                intent_mar = self.monta_intent(intent_mar, intent, 'Condição Pesca', "Hoje não é um bom dia para pescar, pois o mar não está para peixes!")
                
            if infos['altura_maior_onda'] != '':
                intent_mar = self.monta_intent(intent_mar, intent, 'Condição Pesca', "Hoje não é bom dia para pescar, pois o mar não está para peixes!")
                
            else:
                intent_mar = self.monta_intent(intent_mar, intent, 'Condição Pesca', "Hoje não é bom dia para pescar, pois o mar não está para peixes!")
                
            # mar
            intent_mar = self.monta_intent(intent_mar, intent, 'Mar Valor', infos['altura_maior_onda'])
            intent_mar = self.monta_intent(intent_mar, intent, "Mar Anterior", infos['altura_maior_onda_anterior'])
            intent_mar = self.monta_intent(intent_mar, intent, "Mar Média", infos['altura_maior_onda_media'])
            intent_mar = self.monta_intent(intent_mar, intent, "Mar Quantidade dias max", infos['altura_maior_onda_qtd_dias_max'])
            intent_mar = self.monta_intent(intent_mar, intent, "Mar Quantidade dias min", infos['altura_maior_onda_qtd_dias_min'])
            intent_mar = self.monta_intent(intent_mar, intent, "Mar Flag Tendência", infos['altura_maior_onda_flag_tendencia'])
            intent_mar = self.monta_intent(intent_mar, intent, "Mar Diferenca porcentagem", infos['altura_maior_onda_diferenca_porcentagem'])
            intent_mar = self.monta_intent(intent_mar, intent, 'Mar Texto diferenca', infos['altura_maior_onda_texto_diferenca'])
            intent_mar = self.monta_intent(intent_mar, intent, "Mar Flag Outlier", infos['altura_maior_onda_flag_outlier'])
            
            # Navios porto
            intent = 'Navios_porto'
            intent_navios_porto = ''
            intent_navios_porto = self.monta_intent(intent_navios_porto, intent, "Navios porto Valor", infos['qtd_navios_porto'])
            
            if intent_navios_porto != '':
                intent_navios_porto = self.monta_intent(intent_navios_porto, intent, "Navios porto Anterior", infos['qtd_navios_porto_anterior'])
                intent_navios_porto = self.monta_intent(intent_navios_porto, intent, "Mar Média", infos['qtd_navios_porto_media'])
                intent_navios_porto = self.monta_intent(intent_navios_porto, intent, "Navios porto Quantidade dias max", infos['qtd_navios_porto_qtd_dias_max'])
                intent_navios_porto = self.monta_intent(intent_navios_porto, intent, "Navios porto Quantidade dias min", infos['qtd_navios_porto_qtd_dias_min'])
                intent_navios_porto = self.monta_intent(intent_navios_porto, intent, "Navios porto Flag Tendência", infos['qtd_navios_porto_flag_tendencia'])
                intent_navios_porto = self.monta_intent(intent_navios_porto, intent, "Navios porto Diferenca porcentagem", infos['qtd_navios_porto_diferenca_porcentagem'])
                intent_navios_porto = self.monta_intent(intent_navios_porto, intent, 'Navios porto Texto diferenca', infos['qtd_navios_porto_texto_diferenca'])
                intent_navios_porto = self.monta_intent(intent_navios_porto, intent, "Navios porto Flag Outlier", infos['qtd_navios_porto_flag_outlier'])
            
            # Navios chegando
            intent = 'Navios_chegando'
            intent_navios_chegando = ''
            intent_navios_chegando = self.monta_intent(intent_navios_chegando, intent, "Navios chegando Valor", infos['qtd_navios_chegando'])
            
            if intent_navios_chegando != '':
                intent_navios_chegando = self.monta_intent(intent_navios_chegando, intent, "Navios chegando Anterior", infos['qtd_navios_chegando_anterior'])
                intent_navios_chegando = self.monta_intent(intent_navios_chegando, intent, "Navios chegando Média", infos['qtd_navios_chegando_media'])
                intent_navios_chegando = self.monta_intent(intent_navios_chegando, intent, "Navios chegando Quantidade dias max", infos['qtd_navios_chegando_qtd_dias_max'])
                intent_navios_chegando = self.monta_intent(intent_navios_chegando, intent, "Navios chegando Quantidade dias min", infos['qtd_navios_chegando_qtd_dias_min'])
                intent_navios_chegando = self.monta_intent(intent_navios_chegando, intent, "Navios chegando Flag Tendência", infos['qtd_navios_chegando_flag_tendencia'])
                intent_navios_chegando = self.monta_intent(intent_navios_chegando, intent, "Navios chegando Diferenca porcentagem", infos['qtd_navios_chegando_diferenca_porcentagem'])
                intent_navios_chegando = self.monta_intent(intent_navios_chegando, intent, 'Navios chegando Texto diferenca', infos['qtd_navios_chegando_texto_diferenca'])
                intent_navios_chegando = self.monta_intent(intent_navios_chegando, intent, "Navios chegando Flag Outlier", infos['qtd_navios_chegando_flag_outlier'])
                  
            # dict intents
            dict_intents = {'intent_localizacao':intent_localizacao,
                            'intent_tempo':intent_tempo,
                            'intent_temperatura':intent_temperatura,
                            'intent_umidade':intent_umidade,
                            'intent_nebulosidade':intent_nebulosidade,
                            'intent_vento':intent_vento,
                            'intent_mar':intent_mar,
                            'intent_navios_porto':intent_navios_porto,
                            'intent_navios_chegando':intent_navios_chegando
                           }
            
            return dict_intents
            
        except Exception as e:
                print (f'12. Erro! {e}')
                return ""
    
    def popula_estatisticas(self, df_selecionados, df_atual):
        '''
        popula df selecionado com estatísticas envolendo df atual
        '''
        
        # cast
        df_selecionados = df_selecionados.applymap(self.cast_float)
        
        # cast
        df_atual = df_atual.applymap(self.cast_float)
        
        # colunas
        lista_colunas = ['cidade',
                         
                        'temperatura_anterior',
                        'nebulosidade_anterior',
                        'umidade_anterior',
                        'vento_anterior',
                        'altura_maior_onda_anterior',
                        'qtd_navios_porto_anterior',
                        'qtd_navios_chegando_anterior',
                         
                        'temperatura_qtd_dias_max',
                        'nebulosidade_qtd_dias_max',
                        'umidade_qtd_dias_max',
                        'vento_qtd_dias_max',
                        'altura_maior_onda_qtd_dias_max',
                        'qtd_navios_porto_qtd_dias_max',
                        'qtd_navios_chegando_qtd_dias_max',

                        'temperatura_qtd_dias_min',
                        'nebulosidade_qtd_dias_min',
                        'umidade_qtd_dias_min',
                        'vento_qtd_dias_min',
                        'altura_maior_onda_qtd_dias_min',
                        'qtd_navios_porto_qtd_dias_min',
                        'qtd_navios_chegando_qtd_dias_min',

                        'temperatura_media',
                        'nebulosidade_media',
                        'umidade_media',
                        'vento_media',
                        'altura_maior_onda_media',
                        'qtd_navios_porto_media',
                        'qtd_navios_chegando_media',

                        'temperatura_std',
                        'nebulosidade_std',
                        'umidade_std',
                        'vento_std',
                        'altura_maior_onda_std',
                        'qtd_navios_porto_std',
                        'qtd_navios_chegando_std',
                         
                        'temperatura_flag_tendencia',
                        'nebulosidade_flag_tendencia',
                        'umidade_flag_tendencia',
                        'vento_flag_tendencia',
                        'altura_maior_onda_flag_tendencia',
                        'qtd_navios_porto_flag_tendencia',
                        'qtd_navios_chegando_flag_tendencia',
                         
                        'temperatura_texto_diferenca',
                        'nebulosidade_texto_diferenca',
                        'umidade_texto_diferenca',
                        'vento_texto_diferenca',
                        'altura_maior_onda_texto_diferenca',
                        'qtd_navios_porto_texto_diferenca',
                        'qtd_navios_chegando_texto_diferenca',
                         
                        'temperatura_diferenca_porcentagem',
                        'nebulosidade_diferenca_porcentagem',
                        'umidade_diferenca_porcentagem',
                        'vento_diferenca_porcentagem',
                        'altura_maior_onda_diferenca_porcentagem',
                        'qtd_navios_porto_diferenca_porcentagem',
                        'qtd_navios_chegando_diferenca_porcentagem',

                        'temperatura_flag_outlier',
                        'nebulosidade_flag_outlier',
                        'umidade_flag_outlier',
                        'vento_flag_outlier',
                        'altura_maior_onda_flag_outlier',
                        'qtd_navios_porto_flag_outlier',
                        'qtd_navios_chegando_flag_outlier']

        # data
        data_hoje = date.today()
        lista_cidades = list(set(df_selecionados['cidade']))
        
        # se df estiver vazio...
        if (len(df_atual) == 0):
            for cidade in lista_cidades:
                valores = [cidade] + [0 for element in range(len(lista_colunas)-1)]
                df = pd.DataFrame(valores).transpose()
                df.columns = lista_colunas
                lista_valores.append(df)
            return (pd.concat(lista_valores))
            
        # se não estiver vazio...
        else:

            lista_valores = []
            df_atual['data_formatada'] = pd.to_datetime(df_atual['data'])
            df_passado = df_atual.loc[(df_atual['data_formatada'] < pd.to_datetime(data_hoje))]
            
            # itera cidades
            for cidade in lista_cidades:
                df_cidade = df_passado.loc[(df_passado['cidade'] == cidade)]
                lista_datas = set(df_cidade['data'])
                qtd_dias = self.count_dias(data_hoje, lista_datas)
                df_valores_cidade = df_selecionados.loc[(df_selecionados['cidade'] == cidade)].reset_index(drop=True)

                # se poucos dias, valores zerados
                if (qtd_dias < self.qtd_min_dias_consecutivos):
                    valores = [cidade] + [0 for element in range(len(lista_colunas)-1)]
                    df = pd.DataFrame(valores).T
                    df.columns = lista_colunas
                    lista_valores.append(df)
                    continue

                # filtra datas consecutivas
                data_ultima = data_hoje - datetime.timedelta(days=qtd_dias)
                data_ultima = data_ultima.strftime("%d/%m/%Y")
                df_cidade = df_cidade.loc[df_cidade['data_formatada'] >= data_ultima].sort_values(by=['data_formatada'], ascending=False)
                
                data_anterior = data_hoje - datetime.timedelta(days=1)
                data_anterior = data_anterior.strftime("%d/%m/%Y")
                df_cidade_anterior = df_cidade.loc[df_cidade['data_formatada'] >= data_anterior].sort_values(by=['data_formatada'], ascending=False)
                                
                # valores computados
                dict_valores = {}
                for coluna in ['temperatura',
                  'nebulosidade',
                  'umidade',
                  'vento',
                  'altura_maior_onda',
                  'qtd_navios_porto',
                  'qtd_navios_chegando']:
                    
                    # cidade
                    dict_valores['cidade'] = cidade
                    
                    # anterior
                    anterior = df_cidade_anterior[coluna].reset_index(drop=True)[0]
                    dict_valores[f'{coluna}_anterior'] = anterior
                    
                    # max
                    dict_valores[f'{coluna}_qtd_dias_max'] = self.count_valor(df_valores_cidade[coluna].reset_index(drop=True)[0],df_cidade[coluna].values.tolist(), 'maior')
                    
                    # min
                    dict_valores[f'{coluna}_qtd_dias_min'] = self.count_valor(df_valores_cidade[coluna].reset_index(drop=True)[0],df_cidade[coluna].values.tolist(), 'menor')
                    
                    # tendencia
                    try:
                        diferenca = int(100 * (df_valores_cidade[coluna].reset_index(drop=True)[0]/anterior))
                        if diferenca > 110:
                            dict_valores[f'{coluna}_flag_tendencia'] = 'alta'
                        elif diferenca < 90:
                            dict_valores[f'{coluna}_flag_tendencia'] = 'baixa'
                        else:
                            dict_valores[f'{coluna}_flag_tendencia'] = ''
                    except:
                        dict_valores[f'{coluna}_flag_tendencia'] = ''
                        
                        
                    # diferencas
                    dict_valores[f'{coluna}_texto_diferenca'], dict_valores[f'{coluna}_diferenca_porcentagem'] = self.trata_diferenca(df_valores_cidade[coluna].reset_index(drop=True)[0], anterior)
                
                    # outliers
                    try:
                        dict_valores[f'{coluna}_media'] = df_cidade[coluna].mean()
                        dict_valores[f'{coluna}_std'] = df_cidade[coluna].std()
                        limite_superior = dict_valores[f'{coluna}_media'] + (dict_valores[f'{coluna}_std'] * self.multiplicador_std)
                        limite_inferior = dict_valores[f'{coluna}_media'] - (dict_valores[f'{coluna}_std'] * self.multiplicador_std)
                        if (float(df_valores_cidade[coluna][0]) > limite_superior):
                            dict_valores[f'{coluna}_flag_outlier'] = 1
                        elif (float(df_valores_cidade[coluna][0]) < limite_inferior):
                            dict_valores[f'{coluna}_flag_outlier'] = -1
                        else:
                            dict_valores[f'{coluna}_flag_outlier'] = 0
                    except:
                        dict_valores[f'{coluna}_flag_outlier'] = 0
                        
                # append cidade
                lista_valores.append(pd.DataFrame(pd.DataFrame({k: [v] for k, v in dict_valores.items()})))

        
        # estatísticas
        df_estatisticas = pd.concat(lista_valores)

        # resultados
        return df_estatisticas
         
        
    def prepara_lista_tweets(self, texto):
        '''
        retorna lista de textos
        '''
        regex_pattern = r"(?<=[\.\!\?])"
        frases = [frase for frase in re.split(regex_pattern, texto) if len(frase) >= 3]

        combined = []
        while frases:
            items = next((frases[:n] for n in range(len(frases),0,-1) if len(" ".join(frases[:n]))<=270), frases[:1])
            combined.append(" ".join(items))
            frases = frases[len(items):]
        
        combined = [x.strip().replace('.', '. ').replace('  ', ' ').replace('  ', ' ').strip() for x in combined]
        return combined
    
    
    def publica_conteudo(self):
        '''
        Publica previsão do tempo (tábua de marés)
        '''
        
        # data de hoje
        data_hoje_format = date.today().strftime("%d_%m_%Y")
        data_hoje = date.today().strftime("%d/%m/%Y")
      
        try:
            # gera resultados tempo
            df_resultados_tempo = self.gera_df_tempo()
            
            # gera resultados navios
            df_resultados_navios = self.gera_df_navios()
            
            # junta resultados
            df_resultados = pd.merge(df_resultados_tempo, df_resultados_navios, on='cidade', how='left')
            df_resultados['data'] = data_hoje
            
            # salva resultados na pasta
            df_resultados.to_csv(f"resultados_tempo/{data_hoje_format}.csv", index=False, sep=';')
            
            # adiciona dados da rodada ao bd
            df_atual = pd.read_csv(self.path_bd, sep=';')
            df_atual = df_atual.loc[(df_atual['data'] != date.today().strftime("%d/%m/%Y")) & (~df_atual['cidade'].isnull())]
            df_novo = pd.concat([df_atual, df_resultados[self.lista_colunas_salvar]])
            df_novo.to_csv(self.path_bd, index=False, sep=';')
       
            # salva arquivo no mongo db
            for cidade in df_resultados['cidade'].values.tolist():
                try:
                    dict_remove = {'cidade': cidade, 'data': data_hoje}
                    self.collection.delete_many(dict_remove)
                except Exception as e:
                    print (f'1. erro mongo: {e} | cidade: {cidade}')
            
            # salva resultados
            try:
                dict_results = df_resultados.to_dict('records')[0]
                self.collection.insert_one(dict_results)
            except Exception as e:
                    print (f'2. erro mongo: {e}')
            
            # verifica se pode publicar conteúdo
            if (self.twitter_api.get_status_twitter() != 1):
                print ("Flag 0. Não posso publicar!")
                return

            # filtra dados para publicação
            df_selecionados = self.seleciona_conteudo_publicar(df_resultados)       
        
        except Exception as e:
            print (f'13. Erro: {e}')
            sys.exit(0)
        
        # se não encontrar nada para publicar, encerra execução
        try:
            if (len(df_selecionados) == 0):
                (f'14. Erro')
                sys.exit(0)
        except Exception as e:
            print (f'14. Erro: {e}')
            sys.exit(0)
            
        # popula resultados com estatísticas
        df_selecionados = df_selecionados.reset_index(drop=True)
        df_estatisticas = self.popula_estatisticas(df_selecionados, df_atual).reset_index(drop=True).fillna(0)
        
        # junta tabelas
        df_selecionados = pd.merge(df_selecionados, df_estatisticas, how='left', on='cidade')
        df_selecionados = df_selecionados.reset_index(drop=True)
        df_selecionados = df_selecionados.applymap(self.cast_float)
        df_selecionados = df_selecionados.applymap(self.cast_float)
        
        # fillna 0
        df_selecionados = df_selecionados.fillna(0)
        
        lista_input = []
        lista_output = []

        # tenta publicar tweets, um por um
        for index in range(len(df_selecionados)):
            try:
                df_linha = df_selecionados.iloc[index]

                # intents do conjunto de dados
                dict_resultados = self.selecao_conteudo(df_linha)
                
                # check de qualidade
                if dict_resultados == 0:
                    continue
                
                # ordenação do discurso
                dict_resultados = self.ordenacao_discurso(dict_resultados)

                # estruturacao de texto
                dict_resultados = self.estruturacao_texto(dict_resultados)
                lista_input.append(' '.join(list(dict_resultados.values())).strip())
                
                # lexicalizacao
                tweet = self.lexicalizacao(dict_resultados)
                print (tweet)
            
                # caso seja vazio
                if len(tweet) <= 10:
                    continue
                
                # expressoes de referencia
                tweet = self.gera_expressoes_referencia(tweet)
                lista_output.append(tweet)
                
                # realização superficial
                tweet = self.realizacao_superficial(tweet)

                # verifica se o tweet pode ser publicado
                if not self.twitter_api.verifica_tweet_pode_ser_publicado(tweet):
                    continue     
                
                # prepara tweets
                lista_tweets = self.prepara_lista_tweets(tweet)
                len_lista_tweets = len(lista_tweets)
                
                # lista vazia
                if len_lista_tweets == 0:
                    continue

                # flag de publicação
                flag = (random.random() >= 0.5)
                if flag == False:
                    continue
                
                if (len_lista_tweets == 1):
                    # publica tweet
                    self.twitter_api.make_tweet(tweet=tweet,
                                                modulo=self.modulo,
                                                intent="temperatura_navios",
                                                lista_atributos=[],
                                                modo_operacao='padrao',
                                                tweet_id=0)
                    continue
                          
                # itera na lista de tweets
                for indice in range(len_lista_tweets):

                    if indice == 0:
                        # coloca emoji do robô
                        tweet = f"{self.twitter_api.dict_map_emoji['robo']} {lista_tweets[indice]}"
                        id_pub = 0

                    else:
                        id_pub = str(status.id)
                        tweet = f"{lista_tweets[indice]}"

                    # se possui mais texto para publicar
                    if (indice < (len_lista_tweets - 1)):
                        tweet = f"{tweet} {self.twitter_api.dict_map_emoji['tres_pontos']}"

                    # publica tweet
                    status = self.twitter_api.make_tweet(tweet=tweet,
                                            modulo=self.modulo,
                                            intent="temperatura_navios",
                                            lista_atributos=[],
                                            modo_operacao='padrao',
                                            tweet_id=id_pub)
                    time.sleep(self.time_sleep)

                # espera um tempo para publicar novamente
                time.sleep(self.tempo_espera_tweet_segundos)

            except Exception as e:
                print (f"15. Erro: {e}")
                continue
            
        # data de hoje
        data_hoje_format = date.today().strftime("%d_%m_%Y")
        
        # salva rodada input
        print ('salvando resultados...')
        textfile = open(f"input_output/input_{data_hoje_format}.txt", mode="w", encoding="utf-8")
        for element in lista_input:
            textfile.write(element.strip().replace('\n','').replace('\n','').strip() + '\n')
        textfile.close()

        # salva rodada output
        textfile = open(f"input_output/output_{data_hoje_format}.txt", mode="w", encoding="utf-8")
        for element in lista_output:
            textfile.write(element.strip().replace('\n','').replace('..','.').replace('..','.').strip() + '\n')
        textfile.close()