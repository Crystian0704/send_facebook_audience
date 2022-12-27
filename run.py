import time
from pathlib import Path

import fire
import toml

from src.facebook_audience_jobs.download_sql import Create_json
from src.facebook_audience_jobs.send_facebook_batch import Send_facebook_batch


def main(audience_name: str = None):

    """
    Função que chama os objetos de download e envio de dados do Facebook Audience

    params: audience_name: str = None - Nome da audiência, se o nome não for passado,
    será executada todas as audiencias contidas no arquivo config_audience.toml, utilizar o parâmetro -> poetry run python run.py "nome_audiencia"
    para executar uma única audiência.
    Os nomes da audiência podem ser consultadas no arquivo config_audience.toml, utilize sem as chaves externas.

    """

    criar_json = Create_json()

    criar_batch = Send_facebook_batch()

    if audience_name is not None:

        try:
            criar_json.download_data(audience_name)

            criar_batch.send_batch(audience_name)

        except Exception as e:
            print(e)
    else:
        config_file = (
            Path(__file__).parent / 'src' / 'config' / 'config_audience.toml'
        )

        custom_audience_config = toml.load(config_file)

        for audience_name in custom_audience_config:

            time.sleep(2)

            try:
                criar_json.download_data(audience_name)

                criar_batch.send_batch(audience_name)

            except Exception as e:
                print(e)


if __name__ == '__main__':
    fire.Fire(main)
