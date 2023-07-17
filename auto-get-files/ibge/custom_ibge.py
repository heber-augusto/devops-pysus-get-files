import urllib3
import json
import pandas as pd
import requests

class CustomIbge():
    def __int__(self,
                agregador):
        self.agregador = agregador

    def get_metadata_list(self) -> list:

        http = urllib3.PoolManager()

        urlApiIBGE = f'http://servicodados.ibge.gov.br/api/v3/agregados/{self.agregador}/metadados'
        response = http.request('GET', urlApiIBGE)
        data_response = response.data.decode('utf-8')
        data_json = json.loads(data_response)

        if self.agregador == "205":
            data = data_json["classificacoes"][1]["categorias"]

        elif self.agregador == "200":
            data = data_json["classificacoes"][2]["categorias"]
        else:
            raise Exception('classificador inválido')

        ids = [group["id"] for group in data]

        return ids

    def get_files_to_download(self, ids):

        http = urllib3.PoolManager()

        lista_resultados = []

        if self.agregador == "205":

            for grupo_idade in ids:
                urlApiIBGE = f'http://servicodados.ibge.gov.br/api/v3/agregados/205/periodos/1991/variaveis/93?localidades=N6[all]&classificacao=2[4,5]|58[{grupo_idade}]'
                response = http.request('GET', urlApiIBGE)
                data_response = response.data.decode('utf-8')
                data_json = json.loads(data_response)
                print(f'responde status {grupo_idade}: {response.status}')
                for resultado in data_json[0]['resultados']:
                    codigo_sexo = list(resultado['classificacoes'][0]['categoria'].keys())[0]
                    valor_sexo = resultado['classificacoes'][0]['categoria'][codigo_sexo]
                    codigo_categoria = list(resultado['classificacoes'][1]['categoria'].keys())[0]
                    valor_categoria = resultado['classificacoes'][1]['categoria'][codigo_categoria]
                    for serie in resultado['series']:
                        codigo_local = serie['localidade']['id']
                        nome_local = serie['localidade']['nome']
                        ano = list(serie['serie'].keys())[0]
                        valor = serie['serie'][ano]
                        lista_resultados.append(
                            {
                                'codigo_sexo': codigo_sexo,
                                'valor_sexo': valor_sexo,
                                'codigo_local': codigo_local,
                                'nome_local': nome_local,
                                'ano': ano,
                                'populacao': valor,
                                'faixa_idade': valor_categoria
                            }
                        )

        elif self.agregador == "200":
            for grupo_idade in ids:
                urlApiIBGE = f'https://servicodados.ibge.gov.br/api/v3/agregados/200/periodos/2010/variaveis/93?localidades=N6[all]&classificacao=2[4,5]|1[0]|58[{grupo_idade}]'
                response = http.request('GET', urlApiIBGE)
                data_response = response.data.decode('utf-8')
                data_json = json.loads(data_response)
                print(f'responde status {grupo_idade}: {response.status}')
                for resultado in data_json[0]['resultados']:
                    codigo_sexo = list(resultado['classificacoes'][0]['categoria'].keys())[0]
                    valor_sexo = resultado['classificacoes'][0]['categoria'][codigo_sexo]
                    codigo_categoria = list(resultado['classificacoes'][1]['categoria'].keys())[0]
                    valor_categoria = resultado['classificacoes'][1]['categoria'][codigo_categoria]
                    for serie in resultado['series']:
                        codigo_local = serie['localidade']['id']
                        nome_local = serie['localidade']['nome']
                        ano = list(serie['serie'].keys())[0]
                        valor = serie['serie'][ano]
                        lista_resultados.append(
                            {
                                'codigo_sexo': codigo_sexo,
                                'valor_sexo': valor_sexo,
                                'codigo_local': codigo_local,
                                'nome_local': nome_local,
                                'ano': ano,
                                'populacao': valor,
                                'faixa_idade': valor_categoria
                            }
                        )

        df_resultados = pd.DataFrame.from_records(lista_resultados)

        return df_resultados














