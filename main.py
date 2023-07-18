import math
from fastapi import BackgroundTasks, FastAPI
from fastapi.middleware.cors import CORSMiddleware
import requests
import numpy as np
import scipy
import pymongo

mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongo_client["WikiPage"]


class CategoryPageRank:
    url = "https://ru.wikipedia.org/w/api.php"

    def __init__(self, category):
        self.pages_params = {
            'action': 'query',
            'format': 'json',
            'list': 'categorymembers',
            'cmtitle': 'Категория: ' + category,
            'cmlimit': 'max',
            'cmtype': 'page'
        }
        self.links_images_params = {
            'action': 'query',
            'format': 'json',
            'generator': 'categorymembers',
            'gcmtitle': 'Категория: ' + category,
            'gcmlimit': 'max',
            'gcmtype': 'page',
            'prop': 'links|pageimages',
            'piprop': 'thumbnail',
            'pithumbsize': 200,
            'pllimit': 'max'
        }
        self.category_name = category
        self.pages = []
        print('Fetching category...')
        self.fetch_wiki(self.pages_params, self.process_pages_response)
        print('Fetching category done.')
        self.title_to_index = {}
        self.n = len(self.pages)
        self.tp_matrix = np.zeros((self.n, self.n))
        for i in range(self.n):
            self.title_to_index[self.pages[i]['title']] = i
        print('Fetching links...')
        self.fetch_wiki(self.links_images_params, self.process_links_images_response)
        print('Fetching links done.')
        self.tp_matrix = np.apply_along_axis(self.normalize, axis=1, arr=self.tp_matrix).T
        ranks = self.page_rank()
        titled_ranks = {}
        for page in self.pages:
            titled_ranks[page['title']] = ranks[self.title_to_index[page['title']]][0] * 100
        self.titles_sorted = sorted(titled_ranks.items(), key=lambda x: x[1], reverse=True)
        self.result = []
        for title, rank in self.titles_sorted:
            page = self.pages[self.title_to_index[title]]
            self.result.append({
                'title': title,
                'rank': rank,
                'image': page['image'] if 'image' in page else ''
            })

    def process_pages_response(self, response):
        self.pages += response['query']['categorymembers']

    def process_links_images_response(self, response):
        for page in response['query']['pages'].values():
            i = self.title_to_index[page['title']]
            if 'thumbnail' in page and 'image' not in self.pages[i]:
                self.pages[i]['image'] =  page['thumbnail']['source']
            if 'links' in page:
                for link in page['links']:
                    if link['title'] in self.title_to_index:
                        self.tp_matrix[i][self.title_to_index[link['title']]] = 1

    def page_rank(self, b=0.15):
        a = self.tp_matrix * (1 - b) + np.ones((self.n, self.n)) / self.n * b - np.identity(self.n)
        vector = scipy.linalg.null_space(a)
        return vector / vector.sum()

    def fetch_wiki(self, params, f):
        last_continue = {}
        with requests.session() as session:
            while True:
                params_cp = params.copy()
                params_cp.update(last_continue)
                response = session.get(url=self.url, params=params_cp).json()
                f(response)
                if 'continue' not in response:
                    break
                last_continue = response['continue']

    @staticmethod
    def normalize(row):
        s = np.sum(row)
        if s == 0:
            return np.ones(len(row)) / len(row)
        else:
            return row / s

    def get(self):
        return self.result


def rank_category(category):
    print('Creating new instance for', category)
    db['categories'].update_one(
        {
            'title': category
        },
        {'$set': {
            'content': CategoryPageRank(category).get()
        }})
    print('Successfully created new instance for', category)


rank_requested = set()

app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def app_startup():
    db['categories'].update_many(
        {
            'rank_requested': True
        },
        {'$set': {
            'rank_requested': False
        }}
    )


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/categoryrank/")
async def category_rank(category: str, background_tasks: BackgroundTasks, page: int = 0, size: int = 12):
    print('CategoryRank:', category)
    data = db['categories'].find_one({'title': category})
    if 'content' in data:
        offset_min = page * size
        offset_max = (page + 1) * size
        return {
            "total": math.ceil(len(data['content']) / size) - 1,
            "status": 0,
            "data": data['content'][offset_min:offset_max],
        }
    if data['rank_requested'] is True:
        return {
            "total": 0,
            "status": 1,
            "data": [],
        }
    db['categories'].update_one(
        {
            'title': category
        },
        {'$set': {
            'rank_requested': True
        }}
    )
    background_tasks.add_task(rank_category, category)
    return {
        "total": 0,
        "status": 2,
        "data": [],
    }


@app.get("/categories/")
async def get_categories():
    print('Get categories...')
    return list(db['categories'].find({}, {'title': 1, '_id': 0}))
