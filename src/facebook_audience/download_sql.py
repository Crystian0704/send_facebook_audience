import hashlib
import itertools
import json
from dataclasses import dataclass
from pathlib import Path
from random import randint

import numpy as np
import pandas as pd
import sqlalchemy

from src.facebook_audience_jobs.ambiente import Ambiente


@dataclass
class Create_json:

    """
    Objeto que baixa dados dos banco de BI e faz os jsons para envio pro Facebook

    """

    # instancia que chama a classe ambiente para fazer configurações de ambiente
    variaveis_ambiente = Ambiente()

    # conexão com redshift
    _conn_sql = sqlalchemy.create_engine(
        variaveis_ambiente.env_var('CON')
    ).connect()

    def download_data(self, audience_name: str):

        # caminho do arquivo de configuração toml
        config_file = self.variaveis_ambiente.toml_file(
            'src/config/config_audience.toml', audience_name
        )

        # queries usadas para gerar os públicos, separados por arquivos
        sql_files = self.variaveis_ambiente.list_file('src/sql_querie')

        # nome do arquivo sql
        file_name = config_file[2]

        # session_id gerado pra cada sessão de publico de audiencia
        session_id = randint(10000000000, 10000000000000)

        # loop para iserção do público no facebook, limite de 10000 por batch
        for file in sql_files:

            if file_name in str(file):

                sql_querie = file.read_text()

                print('Lendo a querie')

                # constrói objeto iterável com a querie com limite de 10000 por loop
                dataframe = pd.read_sql(
                    sql_querie, self._conn_sql, chunksize=10000
                )

                iterator1, iterator2 = itertools.tee(dataframe)

                # cria uma lista de tamanho por batch
                loop_df = [_.shape[0] for _ in iterator1]

                # quantidade de batch total do público
                chunk = len(loop_df)

                # tamanho total do público
                row = sum(loop_df)

                # cria sessão de publico de audiencia
                session_id = randint(10000000000, 10000000000000)

                print('Iníciando o download dos arquivos json na pasta dados')

                # cria lista de requests
                for index, data in enumerate(iterator2, start=1):

                    params = {
                        'session': {
                            'session_id': session_id,
                            'batch_seq': index,
                            'last_batch_flag': 'false'
                            if index < chunk
                            else 'true',
                            'estimated_num_total': row,
                        },
                        'payload': {
                            'schema': ['EMAIL', 'PHONE'],
                            'data': data.dropna(axis=0, how='all')
                            .replace(np.nan, '')
                            .astype(str)
                            .applymap(
                                lambda x: hashlib.sha256(
                                    x.encode()
                                ).hexdigest()
                            )
                            .values.tolist(),
                        },
                    }

                    caminho = (
                        Path(__file__).parent.parent.parent
                        / 'dados'
                        / str(file_name.strip('.sql'))
                    )
                    caminho.mkdir(parents=True, exist_ok=True)
                    caminho_dados = Path(caminho / f'{index}.txt')

                    # salva os dados em um arquivo txt
                    caminho_dados.write_text(json.dumps(params))
        print('Término do download dos arquivos json na pasta dados')
