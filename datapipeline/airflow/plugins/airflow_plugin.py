from airflow.plugins_manager import AirflowPlugin 
from operators.twitter_operator import TwitterOperator 

class AluraAirflowPlugin(AirflowPlugin):
    name = "alura" # nome da biblioteca. para agrupar operadores e ganchos num mesmo local 
    operators = [TwitterOperator] # a partir daqui, ja da pra testar -> criar uma dag simples 