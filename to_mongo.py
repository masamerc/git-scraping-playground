import datetime
import json
import logging
import os
import requests

from pymongo import MongoClient

# logger setup
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(name=__name__)


if __name__ == '__main__':
    # set up pymongo client
    client = MongoClient(os.environ.get('MONGO_URI'))
    db = client['personal-datalake']
    col = db.github_repos
    
    res = requests.get('https://api.github.com/users/Masamerc/repos')
    repos = res.json()
    logger.info('Inserting {} documents'.format(len(repos)))

    # save response as json
    # formatted_date = datetime.datetime.now().strftime("%Y%m%d_%H:%m:%s")
    with open('masamerc_repos.json', 'w') as f:
        json.dump(repos, f, indent=2)

    # inserting data into / updating data in MongoDB
    for repo in repos:
        repo['requested_at'] = datetime.datetime.now()
        
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