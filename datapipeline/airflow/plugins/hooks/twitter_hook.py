# o gancho, ou hook, vai ser representado como uma classe

from airflow.hooks.http_hook import HttpHook # http hook ja tem os metodos de request necessarios
import requests
import json 

class TwitterHook(HttpHook):

    def __init__(self, query, conn_id = None, start_time = None, end_time = None):
        self.query = query 
        self.conn_id = conn_id or "twitter_default"
        self.start_time = start_time
        self.end_time = end_time

        # inicializar a variavel base url que esta no http hook
        super().__init__(http_conn_id=self.conn_id)
        
    def create_url(self):
        query = self.query
        
        tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,text"
        user_fields = "expansions=author_id&user.fields=id,name,username,created_at"
        #filters = "start_time=2021-12-22T00:00:00.00Z&end_time=2021-12-27T00:00:00.00Z"
        start_time = (
            f"&start_time={self.start_time}"
            if self.start_time 
            else ""
        )
        end_time = (
            f"&end_time={self.end_time}"
            if self.end_time 
            else ""
        )
        url = "{}/2/tweets/search/recent?query={}&{}&{}{}{}".format(
            self.base_url,query, tweet_fields, user_fields,start_time, end_time
        ) 
        return url

    def connect_to_endpoint(self,url, session):
        response = requests.Request("GET", url)
        prep = session.prepare_request(response)
        self.log.info(f"URL:{url}")

        return self.run_and_check(session,prep,{}).json()

    def paginate(self,url, session, next_token=""): # funcao recursiva porque receberemos varias paginas

        if next_token:
            full_url = f"{url}&next_token={next_token}"
        else: # se nao foi a primeira iteracao
            full_url = url
        data = self.connect_to_endpoint(full_url, session) # connect_to_endpoint eh o metodo que retorna uma pagina
        yield data
        if "next_token" in data.get("meta",{}): # next_token indica se ha mais paginas para executar
            yield from self.paginate(url, session, data['meta']['next_token']) #  data['meta']['next_token'] indica qual eh o proximo token


    def run(self):
        session = self.get_conn()

        url = self.create_url()

        yield from self.paginate(url,session)

if __name__=="__main__":
    for pg in TwitterHook("AluraOnline").run():
        print(json.dumps(pg, indent=4,sort_keys=True))
