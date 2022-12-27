from dataclasses import dataclass
from os import getenv
from pathlib import Path
from typing import Any

import toml
from dotenv import load_dotenv


@dataclass
class Ambiente:
    """
    Objeto que define a pasta principal do projeto

    """

    # pasta principal do projeto
    home_project: Any = Path(__file__).parent.parent.parent

    def list_file(self, caminho=home_project):

        """lista todos os arquivos na pasta escolhida,
        se caminho não for colocado será listado todas as
        pastas do home_project"""

        path = self.home_project.joinpath(caminho)
        try:
            return [_ for _ in path.iterdir()]

        except Exception:
            return (
                'Essa pasta não existe no projeto, coloque umas pasta válida'
            )

    def toml_file(self, caminho: str, audience_name: str):

        """
        retorna as configurações do arquivo toml
        passando o nome das audiências que estão no
        arquivo -> config_audience.toml

        return : str

        caminho: caminho do arquivo toml

        key: nome da campanha

        audience_id: id de audiência utilizado no Facebook, a qual a campanha será enviada

        file_name: nome do arquivo a qual a querie está escrita, onde será utilizada para pegar a base no BI

        """

        path = self.home_project.joinpath(caminho)

        try:
            custom_audience_config = toml.load(path)

            for key, value in custom_audience_config.items():

                try:
                    if key == audience_name:

                        audience_id = value['audience_id']
                        file_name = value['file_name']

                        return (key, audience_id, file_name)

                except Exception:
                    return f"""Coloque um nome de campanha válido 
                    entre as campanhas {[_ for _ in custom_audience_config]}"""

        except Exception:
            return 'Coloque um caminho de arquivo toml válido'

    def env_var(self, env: str):

        """Função que retorna uma variável de ambiente
        denfinida no arquivo .env dentro da pasta
        facebook_audience_jobs/src/facebook_audience_jobs"""

        load_dotenv()

        get_env = getenv(env)

        if get_env == None:

            return """Coloque uma variável definida 
                no arquivo .env da pasta 
                facebook_audience_jobs/src/facebook_audience_jobs"""
        else:

            return get_env
