# operador eh uma classe
# todo operador tem um metodo padrao execute

# classe pai:

import json
from datetime import datetime, timedelta
from pathlib import Path
from os.path import join # join ajuda a criar o caminho do arquivo

from airflow.models import DAG, BaseOperator, TaskInstance
from airflow.utils.decorators import apply_defaults # em uma dag podemos ter parametros padrao que vamos mandar para todos os operadores, e o decorator ajuda a aplicar eles
from hooks.twitter_hook import TwitterHook

class TwitterOperator(BaseOperator):

    template_fields = [
        "query",
        "file_path",
        "start_time",
        "end_time"
    ]

    @apply_defaults
    def __init__(
        self,
        # adicionar os parametros que recebemos no gancho, parametros que recebemos do operador e vamos passar pro gancho
        query,
        file_path,
        conn_id = None,
        start_time = None,
        end_time = None,
        *args, **kwargs # carrega um conjunto de argumentos, ou argumentos do tipo chave-valor, entre os metodos
        ):
        super().__init__(*args, **kwargs)
        self.query = query
        self.file_path = file_path
        self.conn_id = conn_id 
        self.start_time = start_time
        self.end_time = end_time 

    # garantir que o arquivo de output esteja sempre disponivel
    # vai olhar o caminho do arquivo, descobrir quais pastas estao faltantes, e vai criar elas
    def create_parent_folder(self):
        Path(Path(self.file_path).parent).mkdir(parents=True, exist_ok=True) # o parametro parent pega apenas o caminho total do arquivo, sem o nome do arquivo no final, recebemos apenas o caminho do arquivo 
    # parentes=True -> vai procurar pastas abaixo do arquivo e verificar se elas precisam ser criadas 
    # exist_ok=True -> nao vai retornar erro se o caminho ja existir 

    def execute(self,context): 
        # metodo chamado no dag para executar o que eh necessario
        # instanciar  a classe twitter hook
        hook = TwitterHook (
            query = self.query,
            conn_id = self.conn_id,
            start_time = self.start_time, 
            end_time = self.end_time
            )

        """
        # imprimir dados na tela
        for pg in hook.run():
            print(json.dumps(pg, indent=4, sort_keys=True))
        """
        self.create_parent_folder()
        # inserir dados num arquivo
        with open(self.file_path,"w") as output_file:
            for pg in hook.run():
                # a funcao json dump nao retorna o json, salva o obj json num arquivo
                json.dump(pg, output_file, ensure_ascii=False) # ensure ascii -> nao perder caracteres
                output_file.write("\n")

if __name__=="__main__":
    # pra testar, vamos criar um dag ficticio
    with DAG(dag_id="TwitterTest",start_date=datetime.now()) as dag:

        to = TwitterOperator(

            query = "AluraOnline",
            file_path=join(
                
                "/home/mvrm/alura-projetos/datapipeline/datalake",
                # tabela 
                "twitter_aluraonline", # cada pasta, uma tabela
                # particoes | no banco de dados nao temos indices para facilitar pesquisas, mas temos particicoes, q num datalake podem ser usado para filtrar pesquisa 
                "extract_date={{ ds }}",
                "AluraOnline_{{ ds_nodash }}.json" # toda vez que executar, quer que reescreva o arquivo com o que esta criando
            # ds_nodash -> ds retorna uma string com a data de execução da tarefa
            # no dash vai tirar pontos e traços e barras 
            ),
            task_id = "test_run"
            # toda vez que um dag instancia uma tarefa, ele instancia a classe taskinstance
        )

        ti = TaskInstance(

            task = to,
            execution_date=datetime.now()
        )

        # com a instacia da tarefa, pode chamar o execute 
        # to.execute(ti.get_template_context())

        # preciso que transforme o template, a instancia da tarefa deve fazer a alteracao
        ti.run() # vai pegar o twitter operator e transformar o nome do arquivo do filepath pro tipo que precisa

# aula 3 | video 1: ate esse ponto, o operador recebe os dados da API  do TT e os imprime na tela
# o proximo passo eh colocar a saida em um arquivo de texto, que sera nosso datalake

# aula 3 | video 2: add file_path -> arquivo onde salvar

"""
apos o video 2 da aula 3:
propriedades:
indepotencia: toda vez que executar, ele vai sempre escrever o mesmo arquivo, sobreescrever se preciso
atomicidade: possui 1 tarefa unica e indivisivel 
isolamento: nao vai conflitar com nenhuma outra tarefa que estiver ocorrendo no momento 

"""

"""
video 3 da aula 2:

- alterar o caminho do arquivo para salvar corretamente no data lake

"""