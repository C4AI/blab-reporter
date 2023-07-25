from datetime import date
import pandas as pd
import random
import json
import sys
import os
from twitter_api import TwitterClass

class CuriosidadesClass:
    """
    Classe de curiosidades
    """
    def __init__(self):
        
        # API do Twitter
        self.twitter_api = TwitterClass()
        
        # get meses
        self.dict_map_mes = self.twitter_api.get_meses()
        
        # dia atual
        print (self.get_dia_atual())
        
        # path atual
        self.current_path = str(os.getcwd())
        
        # path json curiosidades
        path_curiosidades = os.path.join(self.current_path, "curiosidades.csv")
        
        # modulo
        self.modulo = 'curiosidades'
        
        # curiosidades
        with open(path_curiosidades) as f:
            self.lista_curiosidades = [linha.replace('\n', '') for linha in f.readlines()]
    
    def seleciona_curiosidade(self):
        '''
        Seleciona curiosidade da lista
        '''
        # tenta selecionar elemento aleatório da lista
        for tentativa in range(20):
            curiosidade = random.choice(self.lista_curiosidades)
            tweet = f"{self.twitter_api.get_inicio_post()}Você sabia?\n{curiosidade}\n\n{self.get_dia_atual()}{self.twitter_api.get_fim_post()}"
            flag_pode_ser_publicado = self.twitter_api.verifica_tweet_pode_ser_publicado(tweet)
            if flag_pode_ser_publicado == 1:
                return 1, tweet
            
        # não encontrou curiosidades novas
        print ('Não encontrei curiosidades novas')
        sys.exit(0)
    
    
    def get_dia_atual(self):
        '''
        data de hoje
        '''
        # data de hoje
        dia = date.today().strftime("%d")
        mes = self.dict_map_mes[int(date.today().strftime("%m"))]
        ano = date.today().strftime("%Y")
        return f"{dia} de {mes} de {ano}"
    
    
    def prepara_tweet(self):
        '''
        Prepara tweet
        '''
        flag_pode_ser_publicado, tweet = self.seleciona_curiosidade()
        if flag_pode_ser_publicado == 0:
            return 0, ""
        
        # verifica se tamanho está ok
        if self.twitter_api.valida_tamanho_tweet(tweet) != 1:
            return 0, ""
        
        return 1, tweet
    
    
    def publica_conteudo(self):
        '''
        Tenta publicar curiosidade
        '''
        
        try:
            # flag de publicação
            if (self.twitter_api.get_status_twitter() != 1):
                print ("Flag 0. Não posso publicar!")
                return

            # seleciona curiosidade para publicar
            flag, tweet = self.prepara_tweet()

            # tweet deu errado
            if flag == 0:
                sys.exit(0)
             
        except Exception as e:
            print (e)

        # tenta publicar 
        try:
            self.twitter_api.make_tweet(tweet=tweet,
                                        modulo=self.modulo,
                                        intent="curiosidades",
                                        lista_atributos=[],
                                        modo_operacao='padrao',
                                        tweet_id=0)
            print ('Tweet publicado!')

        # algo deu errado
        except Exception as e:
            print (f'Erro! {e}')