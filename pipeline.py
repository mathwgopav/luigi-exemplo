import luigi
import requests
import pandas as pd
import time
import json

class BaixaMunicipiosCE(luigi.Task):
    def output(self):
        return luigi.LocalTarget('municipios_ce.json')

    def run(self):
        url = 'https://servicodados.ibge.gov.br/api/v1/localidades/estados/23/municipios'
        response = requests.get(url)
        response.raise_for_status()

        with self.output().open('w') as f:
            f.write(response.text)

class BaixaPopulacao(luigi.Task):
    def requires(self):
        return BaixaMunicipiosCE()

    def output(self):
        return luigi.LocalTarget('populacao_ce.csv')

    
    def run(self):

        with self.input().open('r') as f:
            data = json.load(f)  # agora lê corretamente
            municipios = pd.DataFrame(data)

        dados = []
        for _, row in municipios.iterrows():
            nome = row['nome']
            id_municipio = row['id']
            try:
                url = f'https://servicodados.ibge.gov.br/api/v1/projecoes/populacao/{id_municipio}'
                r = requests.get(url)
                r.raise_for_status()
                populacao = r.json()['projecao']['populacao']
                dados.append({'municipio': nome, 'populacao': populacao})
                time.sleep(1)
            except Exception as e:
                print(f"Erro com {nome}: {e}")

        df = pd.DataFrame(dados)
        df.to_csv(self.output().path, index=False)



if __name__ == '__main__':
    luigi.build([BaixaPopulacao()], local_scheduler=True, workers=1, detailed_summary=True)
