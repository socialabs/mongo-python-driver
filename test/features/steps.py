# Copyright 2011 10gen, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from freshen import *
import json
import pymongo

@Transform('^"(.+)" database$')
def transform_database_name(database_name):
    """
    Assuming feature file is encoded in utf-8, unicodify all database names
    """
    import sys
    print >> sys.stderr, 'database_name', database_name
    return database_name.decode('utf-8')

@Transform('^"(.+)" collection$')
def transform_collection_name(collection_name):
    """
    Assuming feature file is encoded in utf-8, unicodify all collection names
    """
    collection_name = collection_name.decode('utf-8')
    import sys
    print >> sys.stderr, 'collection_name', collection_name
    return collection_name

@Given('connection to "(.*)"')
def connection(cx_str):
    scc.connection = pymongo.Connection(cx_str)

@Given('(".*" database) selected')
def select_db(db_str):
    scc.db = scc.connection[db_str]

@Given('(".*" database) truncated')
def truncate_db(db_str):
    for c in scc.db.collection_names():
        if not c.startswith('system.'):
            scc.db.drop_collection(c)

@When('(".*" collection) created')
def create_collection(collection_name):
    scc.db.create_collection(collection_name)

@When('(".*" collection) selected')
def select_collection(collection_name):
    scc.collection = scc.db[collection_name]

@When('(".*" collection) truncated')
def truncate_collection(collection_name):
    scc.db[collection_name].remove()

@When('(".*" collection) dropped')
def drop_collection(collection_name):
    scc.db.drop_collection(collection_name)

@When('following documents inserted')
def insert_documents(table):
    scc.collection.insert(table.iterrows())

@When('find documents by')
def find(query):
    scc.find_result = list(scc.collection.find(json.loads(query)))

@When('add "(.*)" index by')
def create_index(index_params, index_name):
    scc.collection.create_index(
        json.loads(index_params).items(),
        name=index_name,
    )

@Then('list of collections would be the following')
def check_collections(table):
    collections = set(scc.db.collection_names())
    assert collections == set(row['name'] for row in table.iterrows())

@Then('result would be empty')
def check_empty():
    assert [ ] == scc.find_result
