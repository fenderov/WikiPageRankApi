import pymongo
import requests


def fetch_categories():
    c = []
    url = "https://ru.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "format": "json",
        "list": "allcategories",
        'aclimit': 'max',
        'acmin': 500
    }
    last_continue = {}
    with requests.session() as session:
        while True:
            params_cp = params.copy()
            params_cp.update(last_continue)
            response = session.get(url=url, params=params_cp).json()
            for page in response["query"]["allcategories"]:
                c.append({
                    'title': page['*'],
                    'rank_requested': False
                })
            if 'continue' not in response:
                break
            last_continue = response['continue']
    return c


mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongo_client["WikiPage"]
db['categories'].drop()
db['categories'].insert_many(fetch_categories())