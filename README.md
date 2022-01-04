# datapipeline
Datapipeline created using Apache Airflow and Apache Spark - project from an Alura's course.<br>
Below is a summary of what was developed during the course and the main concepts presented.

# Conceitos
### Data pipeline: 
cadeia de workflows que manipulam dados, seguindo o formato ETL/ELT.
Possuem pontos de extração, opções de transformação e um local de carregamento. 
Movimentar dados de várias fontes para um ou mais destinos

### Batch: 
lotes/pacotes
Agrupamos dados para serem processados de tempos em tempos
O pacote é movido pelo pipeline de tempos em tempos
### Streaming: 
fluxo
Fluxos de dados constantes, que são processados assim que dados são gerados. Processamento em tempo real

### Data lake: 
repositório de arquivos. Podem ser estruturados, semi-estruturados e não estruturados.
Armazenamento quase ilimitado.
É preciso utilizar ferramentas para organizar os dados, o que não acontece em um banco de dados, que tem SGBD.
Data Lakes podem ser estados intermediários, onde os dados ficam armazenados antes de ir pro Datawarehouse.
### Data Warehouse 
É um sistema de business intelligence utilizado para gerar relatórios e análises. Costuma ter uma organização como um SGBD.
### ETL:
 reprocessamento fácil em caso de erro na transformação, pois o dado já foi carregado na sua forma bruta.
### DAG: 
gráfico acíclico direcionado. Permite a execução de vários processos de extração em paralelo.
O DAG interrompe as tarefas seguintes e reprocessa a tarefa atual que falhou.
Os passos são executados através de um operador, que tem 3 características:
1- Idempotência: sempre o mesmo resultado de uma tarefa, independente de quantas vezes ela seja executada. 
2- Isolamento: as tarefas não podem compartilhar a mesma estrutura com outras tarefas.
3- Atomicidade: cada operador tem uma tarefa única de indivisível. No caso da ETL, cada processo é indivisível.

### 3Vs do Big Data: 
volume, velocidade (real time ou batch), variedade (estruturado ou não)
### Data Lake:
 sistema de arquivos distribuídos que utiliza ferramentas para armazenar e processar dados. Exemplo: Hadoop HDFS (sistema de arquivos distribuídos do Hadoop).

### RDB: 
conjunto de dados distribuídos resilientes. Conjunto de dados mais simples que temos no Spark.

# Projeto
Extrair dados do Twitter para que uma equipe possa analisar os dados em busca de informações de engajamento.

**Projeto deve garantir:**
Escalonamento: possibilitar extrair mais dados, adicionar mais processamento, memória ou armazenamento.
Automação: orquestrar o processo em uma plataforma que permita a automação de cada tarefa
Monitoramento: de trabalho e monitoramento de logs e alertas
Manutenção: os itens acima diminuem o tempo de manutenção, investigação de falhas e adição de novos trabalhos
Expansão: novas fontes de dados devem poder ser usadas

### Data pipeline
 Verificar se a fonte de dados está acessível -> Twitter API
Fazer uma conta de developer, criar projeto e salvar as chaves de acesso
 Usaremos o modelo batch: solicitaremos dados da API em um período determinado
Batch e ELT (armazenamento do dado em seu formato bruto, antes da transformação).

Após a carga, vamos usar uma ferramenta de processamento distribuído (Apache Spark) para transformar os dados em um formato estruturado.

### Apache Airflow: orquestração
Agendar execuções e monitorar o estado de cada uma.
Principal orquestrador de workflow na Google Cloud Composer. 
MWAA na AWS.
Desenvolvido em Python.
**DAG**: gráfico acíclico direcionado. Permite a execução de vários processos de extração em paralelo.
O DAG interrompe as tarefas seguintes e reprocessa a tarefa atual que falhou.
Serviços:
Webserver: UI e interface de controle.
Tem a lista de DAGs, informações e gráficos de execução.
Scheduler: verifica as DAGs e agenda a execução dos próximos passos em cada um dos trabalhos.
### Apache spark:
 processamento distribuído. Motor analítico unificado para processamento distribuído de dados. Desenvolvido em Scala.
Usando a biblioteca spark.sql é possível fazer pesquisar usando SQL e dataframes.

### Conexão
Criamos uma conexão no Apache Airflow com a API do Twitter. Informamos o tipo da conexão (HTTP), o host e uma chave de acesso fornecida pela API do Twitter.

### Gancho:
Um gancho/hook permite que criemos a interface necessária para interagir com a API.
No caso desse projeto, o hook será uma classe com os métodos necessários para interagir com a API, como métodos de request herdados da classe HttpHook do airflow.hooks.

### Operadores: 
ficam na pasta plugins/operators
Operador será uma classe, no caso desse projeto.  A classe pai será a BaseOperator de airflow.models.

A cada tarefa, pegamos os dados e os salvamos, pois o Airflow não salva dados.

### Data lake do projeto
O Data lake  do projeto será o sistema de arquivos local.
No twitter_operator.py, editamos o file_path para que o arquivo seja salvo no sistema local. 

### Extração e transformação

Criamos um dag com operador específico para extrair dados do Twitter.

Na pasta Spark, o arquivo tranformation.py contém o código que possibilita que o spark leia os dados e os gere no formato de tabela.

Usando o Spark, transformamos o rdd em um dataframe.
Usando explode, no Spark, “explodimos” um array. Para cada linha do array, é criada uma nova linha. Assim, podemos extrair os campos do array separadamente.


Os dados podem ser divididos em partições utilizando, no spark, a função repartition ou coalesce.
Usando repartition, os dados são reorganizados no número de partições indicadas, e pode ser usado para aumentar ou diminuir o número de partições. Já o coalesce junta partições em uma, e é usado para diminuir o número de partições.
Usando a função partitionBy(parâmetro), podemos particionar a saída por data de criação (created_date) de cada arquivo.

### Organização dos dados no formato de medalhas

Iremos organizar os dados em 3 pastas no formato de medalhas: bronze, silver e gold. 
Na pasta bronze, ficarão os dados brutos, nas pasta silver colocamos os dados transformados e na pasta gold os dados prontos para análise.

### Detalhes finais 

No twitter_dag.py, adicionamos um caminho para o transformation.py na task de transformação.

No DAG, precisamos explicitar a frequência com a qual ele é executado e conectar as atividades ( twitter_aluraonline e transform). Usando colchetes angulares, conectamos tarefas (>>). Para explicitar a frequência, informamos o start_time o end_time.

Na camada bronze, temos os dados extraídos do Twitter e na camada bronze temos os dados separados por user e tweets. Por fim, na camada gold, colocaremos dados de insight. O arquivo que irá extrair as informações da camada silver está dentro da pasta spark.
