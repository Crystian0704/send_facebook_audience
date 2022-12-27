import json
import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from shutil import rmtree

from facebook_business.adobjects.customaudience import CustomAudience
from facebook_business.api import FacebookAdsApi

from src.facebook_audience_jobs.ambiente import Ambiente


@dataclass
class Send_facebook_batch:

    """
    Objeto que baixa dados dos banco de BI e faz os jsons para envio pro Facebook

    """

    # instancia que chama a classe ambiente para fazer configurações de ambiente
    variaveis_ambiente = Ambiente()

    # token de acesso Facebook
    _access_token = variaveis_ambiente.env_var('ACESS_TOKEN')

    # chave secreta do aplicativo de Facebook
    _app_secret = variaveis_ambiente.env_var('APP_SECRET')

    # id do aplicativo Facebook
    _app_id = variaveis_ambiente.env_var('APP_ID')

    # conexão com api do Facebook audience
    _connection_api = FacebookAdsApi.init(access_token=_access_token)

    def send_batch(self, audience_name: str):

        # caminho do arquivo de configuração toml
        config_file = self.variaveis_ambiente.toml_file(
            'src/config/config_audience.toml', audience_name
        )

        # audience_id do público que está no arquivo toml
        audience_id = config_file[1]

        # nome do arquivos de dados
        file_name = config_file[2]

        # instância o POST de audiencia no Facebook
        audience = CustomAudience(audience_id)

        next_batch = self._connection_api.new_batch()

        # lista de responses do batch
        batch_body_responses = []

        # função response usada no request do batch
        def success_callback(response):
            batch_body_responses.append(response.body())

        data = Path(file_name.strip('.sql'))

        caminho_dados = Path(__file__).parent.parent.parent / 'dados' / data

        lista_dados = self.variaveis_ambiente.list_file('dados' / data)

        requests = []

        tamanho_base = len(lista_dados)

        index = 0

        inicio_envio = datetime.now()
        print(
            f'Inicio do envio: {inicio_envio}, da audiência: {audience_name}'
        )

        # loop para iserção do público no facebook, limite de 10000 por envio
        for count, json_param in enumerate(
            sorted(lista_dados, key=os.path.getmtime), start=1
        ):

            params = json_param.read_text()
            data_dict = json.loads(params)
            request = audience.create_users_replace(
                params=data_dict, batch=next_batch, success=success_callback
            ) 

            # adiciona request na lista de requests
            requests.append(request)
            index += 1
            json_param.unlink()
            if index >= 22:

                try:
                    next_batch.execute()
                    time.sleep(0.5)
                    requests = []
                    print(f'Enviados {count}, faltam {tamanho_base - count}')
                    index = 0
                    next_batch = self._connection_api.new_batch()

                except Exception as e:
                    print(e)
            else:
                pass

        # executa o batch com requests
        next_batch.execute()
        print(f'Enviados {count}')

        # apaga a pasta de dados
        rmtree(caminho_dados, ignore_errors=True)

        fim_envio = datetime.now()
        print(f'Fim do envio: {fim_envio}')

        duracao = fim_envio - inicio_envio
        print(f'Duração do envio: {duracao}')

        chave_de_execucao = datetime.now().strftime('%Y%m%d-%H%M%S')

        caminho = Path(__file__).parent.parent.parent / 'logs'

        # logs do envio
        caminho.mkdir(parents=True, exist_ok=True)
        caminho_logs = Path(caminho / f'{file_name.strip(".sql")}.log')
        # caminho_logs.write_text("\n"f'{chave_de_execucao} --- {json.dumps(batch_body_responses)}')
        with open(caminho_logs, 'a') as log:
            log.write('\n')
            log.write(
                f'Datetime: {chave_de_execucao}, Duração: {duracao}, primeiro envio json: {batch_body_responses[0]}'
            )
