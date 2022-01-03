import requests
import os
import json

# To set your enviornment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'


def auth():
    return os.environ.get("BEARER_TOKEN")


def create_url():
    query = "AluraOnline"
    # Tweet fields are adjustable.
    # Options include:
    # attachments, author_id, context_annotations,
    # conversation_id, created_at, entities, geo, id,
    # in_reply_to_user_id, lang, non_public_metrics, organic_metrics,
    # possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets,
    # source, text, and withheld
    tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,text"
    user_fields = "expansions=author_id&user.fields=id,name,username,created_at"
    filters = "start_time=2021-12-22T00:00:00.00Z&end_time=2021-12-27T00:00:00.00Z"
    url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}&{}&{}".format(
        query, tweet_fields, user_fields, filters
    )
    return url


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def connect_to_endpoint(url, headers):
    response = requests.request("GET", url, headers=headers)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def paginate(url, headers, next_token=""): # funcao recursiva porque receberemos varias paginas

    if next_token:
        full_url = f"{url}&next_token={next_token}"
    else: # se nao foi a primeira iteracao
        full_url = url
    data = connect_to_endpoint(full_url, headers) # connect_to_endpoint eh o metodo que retorna uma pagina
    yield data
    if "next_token" in data.get("meta",{}): # next_token indica se ha mais paginas para executar
        yield from paginate(url, headers, data['meta']['next_token']) #  data['meta']['next_token'] indica qual eh o proximo token

def main():
    bearer_token = auth()
    url = create_url()
    headers = create_headers(bearer_token)
    for json_response in paginate(url, headers):
        #json_response = connect_to_endpoint(url, headers) # paginacao
        print(json.dumps(json_response, indent=4, sort_keys=True))


if __name__ == "__main__":
    main()

