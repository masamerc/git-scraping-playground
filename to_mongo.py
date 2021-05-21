import datetime
import logging
import os
import requests

from pymongo import MongoClient

# logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(name=__name__)


client = MongoClient(os.environ.get('MONGO_URI'))

res = requests.get('https://api.github.com/users/Masamerc/repos')
repos = res.json()

for repo in repos:
    repo['requested_at'] = datetime.datetime.now()


if __name__ == '__main__':
    db = client['personal-datalake']
    col = db.github_repos

    logger.info('Inserting {} documents'.format(len(repos)))

    for repo in repos:

        if col.find({'name': repo['name']}):
            logger.info('Document with name {} found. Updating'.format(repo['name']))
            col.update_one(
                {'name': repo['name']},
                {'$set': {
                    'requested_at': repo['requested_at']
                }},
                upsert=True
            )
        
        else:
            col.insert_one(repo)