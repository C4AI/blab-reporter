import pandas as pd
import random
import time
import json
import cv2
import sys
import os
import datetime
from datetime import date
from bs4 import BeautifulSoup
from discourse_ordering import DiscourseOrderingClass
from twitter_api import TwitterClass
from shapely.geometry.polygon import Polygon
from shapely.geometry import Point
import requests
import csv

class TerremotosClass:
    """
    Classe de imagens de satélite
    """
    def __init__(self):
        
        # mapeamento de meses
        self.dict_map_mes = {1: 'janeiro',
                             2: 'fevereiro',
                             3: 'março',
                             4: 'abril',
                             5: 'maio',
                             6: 'junho',
                             7: 'julho',
                             8: 'agosto',
                             9: 'setembro',
                             10: 'outubro',
                             11: 'novembro',
                             12: 'dezembro'
                             }
        
        # dia atual formatado
        self.dia_atual_format = date.today().strftime("%Y-%m-%d")
        
        # dia atual
        print (self.get_dia_atual())
        
        # path atual
        self.current_path = str(os.getcwd())
        
        # API do Twitter
        self.twitter_api = TwitterClass()
        
        # arquivos auxiliares
        self.path_bd = os.path.join(self.current_path, "terremotos_bd.csv")
        path_intents = os.path.join(self.current_path, "intents.json")
        path_analisador_lexico = os.path.join(self.current_path, "analisador_lexico.json")
        self.discourse_ordering_object = DiscourseOrderingClass()
        
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
        self.tempo_espera_tweet_segundos = 60
        self.qtd_max_terremotos = 10
        self.modulo = 'terremotos'
        
        # colunas para atribuir valor
        self.lista_colunas = ['id_terremoto',
                              'data_info',
                              'localizacao',
                              'magnitude',
                              'magtype',
                              'profundidade',
                              'lat',
                              'lon']
        
        # se não existe arquivo de bd, cria
        if not os.path.exists(self.path_bd):
            pd.DataFrame(columns=self.lista_colunas).to_csv(self.path_bd, sep=';', index=False)
        
        # data de hoje
        self.data_hoje = self.get_dia_atual()
        
        # área de interesse
        self.area_interesse = Polygon([(18.09, -39.05),
                                       (-1.28, -15.92),
                                       (-20.76, -8.2),
                                       (-38.24, -18.7),
                                       (-32.50, -53.4),
                                       (-27.518, -49.5),
                                       (-24.11, -47.9),
                                       (-21.93, -42.7),
                                       (-11.87, -39.3),
                                       (-7.04, -36.3),
                                       (-2.16, -50.9),
                                       (3.28, -52.35)])
        
    
    def get_dia_atual(self):
        '''
        data de hoje
        '''
        # data de hoje
        dia = str(int(date.today().strftime("%d")))
        mes = self.dict_map_mes[int(date.today().strftime("%m"))]
        ano = str(int(date.today().strftime("%Y")))
        return f"{dia} de {mes} de {ano}"
        
   
    def extrai_resultados(self):
        '''
        Gera resultados
        '''
    
        try:
            lista_infos = []
            
            # inclui data de hoje
            data_hoje = date.today().strftime("%d/%m/%Y")
        
            # entra na url do site e realiza leitura do arquivo csv
            url = "http://www.moho.iag.usp.br/fdsnws/event/1/query?format=text&orderby=time&limit=100&maxlat=10&minlon=-55&maxlon=-10&minlat=-55"
            driver = requests.Session()
            time.sleep(1)
            page = requests.Session()
            download = page.get(url)
            time.sleep(2)
            decoded_content = download.content.decode('utf-8')
            cr = csv.reader(decoded_content.splitlines(), delimiter='|')
            my_list = list(cr)
            lista_colunas = ['#EventID', 'Time', 'Latitude', 'Longitude', 'Depth/km', 'MagType', 'Magnitude', 'EventLocationName']
            df = pd.DataFrame(my_list[1::], columns=my_list[0])[lista_colunas]

            # itera na lista de terremotos
            for index, row  in df.iterrows():
                id_terremoto = str(row['#EventID'])
                data_info = str(row['Time']).strip().split('T')[0]
                ano = data_info.split('-')[0]
                mes = self.dict_map_mes[int(data_info.split('-')[1])]
                dia = data_info.split('-')[2]
                data = f'{dia} de {mes} de {ano}'
                lat = float(str(row['Latitude']).strip())
                long = float(str(row['Longitude']).strip())
                profundidade = round(float(str(row['Depth/km']).strip()), 2)
                magnitude = round(float(str(row['Magnitude']).strip()), 2)
                magtype = str(row['MagType']).strip()
                localizacao = str(row['EventLocationName']).strip()
                
                # verifica se o evento foi hoje
                if (data_info != self.dia_atual_format):
                    continue

                # verifica se ponto está na área de interesse
                ponto = Point(lat, long)
                if not self.area_interesse.contains(ponto):
                    continue
                        
                # ignora se profundidade <= 0
                if float(profundidade) <= 0:
                    continue
                
                # salva valores na lista
                lista_infos.append([id_terremoto,
                                    data,
                                    localizacao,
                                    magnitude,
                                    magtype,
                                    profundidade,
                                    lat,
                                    long])
            

            # adiciona listas
            df_infos = pd.DataFrame(lista_infos,
                                    columns=self.lista_colunas)

            # transforma tudo em string
            for column in df_infos.columns.tolist():
                df_infos[column] = df_infos[column].astype(str)

            # retorna resultados
            return df_infos

        # erro de execução
        except Exception as e:
            print (f'Erro! {e}')
            driver.close()
            sys.exit(0)

     
    def seleciona_conteudo_publicar(self, df_resultados):
        '''
        Seleciona conteúdo para publicar, de acordo com a estratégia implementada
        '''

        try:
            # estratégia de seleção de conteúdo
            max_observacoes = min(len(df_resultados), self.qtd_max_terremotos)
            df_selecionados = df_resultados.sample(max_observacoes)

            # retorna resultados selecionados
            return df_selecionados
        except:
            sys.exit(0)
    

    def atribui_intent(self, df_linha):
        '''
        retorna intent com base no conteúdo
        '''
        
        try:
            magnitude = float(df_linha['magnitude'])
            
            # escala 5
            if magnitude >= 8.0:
                return "TERREMOTO_5"
            
            # escala 4
            if magnitude >= 7.0:
                return "TERREMOTO_4"
            
            # escala 3
            if magnitude >= 6.1:
                return "TERREMOTO_3"
            
            # escala 2
            if magnitude >= 5.5:
                return "TERREMOTO_2"
            
            # escala 1
            if magnitude >= 2.5:
                return "TERREMOTO_1"
            
            # escala 0
            return "TERREMOTO_0"
            
        except Exception as e:
                print (f'Erro! {e}')
                return ""
    
    def atribui_template(self, df_linha, intent):
        '''
        Retorna template
        '''
        print (f'Intent: {intent}')
        
        try:
            # campos principais
            localizacao = df_linha['localizacao']
            magnitude = df_linha['magnitude']
            profundidade = df_linha['profundidade']
            lat = df_linha['lat']
            lon = df_linha['lon']
                
            # lista de textos possíveis para cada intent
            lista_possibilidades = self.dict_intents[intent]
            
            # seleciona texto
            texto_selecionado = random.choice(lista_possibilidades).strip()
            
            # dicionário para substituição de campos
            dicionario_map = {
                              "[localizacao]":f'"{localizacao}"',
                              "[magnitude]":str(magnitude).replace(".",","),
                              "[profundidade]":str(int(round(float(profundidade)))).replace(".",","),
                              "[data_hoje]":self.data_hoje
                             }
            
            # aplica substituições no template
            for key, value in dicionario_map.items():
                texto_selecionado = texto_selecionado.replace(key, value)
            
            # adiciona pós-processamentos ao tweet
            tweet = f"{self.twitter_api.get_inicio_post()}{texto_selecionado.strip()}{self.twitter_api.get_fim_post()}"

            return tweet
        
        
        except Exception as e:
            print (f'Erro! {e}')
            return ""
    
        
    def publica_conteudo(self):
        '''
        Publica terremotos
        '''
        
        # data de hoje
        data_hoje = date.today().strftime("%d_%m_%Y")
        
        try:
            # gera resultados
            df_resultados = self.extrai_resultados()
            
            # se o conteúdo for de tamanho 0, retorna
            if len(df_resultados) == 0:
                return
            
<<<<<<< HEAD
            # adiciona dados da rodada ao BD
            df_atual = pd.read_csv(self.path_bd, sep=';')

#             # filtra informações que ainda não estão no bd
#             outer_join = df_resultados.merge(df_atual, how='outer', indicator=True, on=['data_info', 'localizacao'])
#             df_dados_novos = outer_join[~(outer_join._merge == 'both')].drop('_merge', axis = 1)
#             df_dados_novos = df_dados_novos.iloc[:, :5]
#             df_dados_novos.columns = self.lista_colunas
            
#             # adiciona dados ao bd atual
#             try:
#                 df_novo = df_atual.append(df_dados_novos[self.lista_colunas])
#                 df_novo.to_csv(self.path_bd, index=False, sep=';')
#             except Exception as e:
#                 print (e)
=======
            df_resultados = pd.DataFrame(['1', '1', '1', '1', '1']).transpose()
            df_resultados.columns = self.lista_colunas
            print (df_resultados)
            
            # adiciona dados da rodada ao BD
            df_atual = pd.read_csv(self.path_bd, sep=';')

            # filtra informações que ainda não estão no bd
            outer_join = df_resultados.merge(df_atual, how='outer', indicator=True, on=['data_info', 'localizacao'])
            df_dados_novos = outer_join[~(outer_join._merge == 'both')].drop('_merge', axis = 1)
            df_dados_novos = df_dados_novos.iloc[:, :5]
            df_dados_novos.columns = self.lista_colunas
            
            # adiciona dados ao bd atual
            try:
                df_novo = df_atual.append(df_dados_novos[self.lista_colunas])
                df_novo.to_csv(self.path_bd, index=False, sep=';')
            except Exception as e:
                print (e)
>>>>>>> 3baf2b5123e8124a9e0c7d55812a9a288ff5e6b3

            # filtra dados para publicação
            df_selecionados = self.seleciona_conteudo_publicar(df_resultados)
        
        except:
            sys.exit(0)
        
        # se não encontrar nada para publicar, encerra execução
        try:
            if (len(df_selecionados) == 0):
                sys.exit(0)
        except:
            sys.exit(0)
                        
        # verifica se pode publicar conteúdo
        if (self.twitter_api.get_status_twitter() != 1):
            print ("Flag 0. Não posso publicar!")
            return
        
        # tenta publicar tweets, um por um
        for index in range(len(df_selecionados)):
            try:
                # informações
                df_linha = df_selecionados.iloc[index]

                # intent do conjunto de dados
                intent = self.atribui_intent(df_linha)
                if (intent == ""):
                    print ('intent vazio. tweet não pode ser publicado.')
                    continue

                # cria o tweet
                tweet = self.atribui_template(df_linha, intent)

                # verifica se pode publicar o tweet
                if (tweet == ""):
                    print ('tweet vazio. tweet não pode ser publicado.')
                    continue

                lista_atributos = ', '.join(df_linha.values.tolist())
                # verifica se tweet pode ser publicado                
                if (self.twitter_api.valida_tweet(tweet)):
                    try:
                        self.twitter_api.make_tweet(tweet, self.modulo, intent, lista_atributos, 'padrao')
                        
                        # espera um tempo para publicar novamente
                        print ('Tweet publicado')
                        time.sleep(self.tempo_espera_tweet_segundos)

                    except Exception as e:
                        print ('Não consegui publicar.')
                        print (f"Erro: {e}")

                # tweet não pode ser publicado
                else:
                    print ('tweet não pode ser publicado')

            # continua a execução
            except Exception as e:
                print (f"Erro na publicação! {e}")
                continue