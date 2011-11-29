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

@Given('connection to "(.*)"')
def connection(cx_str):
    scc.connection = pymongo.Connection(cx_str)

@Given('"(.*)" database selected')
def select_db(db_str):
    scc.db = scc.connection[db_str]

@When('"(.*)" collection selected')
def select_collection(collection_str):
    scc.collection = scc.db[collection_str]

@When('"(.*)" collection truncated')
def truncate_collection(collection_str):
    scc.collection.remove()

@When('"(.*)" collection dropped')
def drop_collection(collection_str):
    scc.db.drop_collection(collection_str)

@When('following documents inserted')
def insert_documents(table):
    scc.collection.insert(table.iterrows())

@When('find documents by')
def find(query):
    scc.find_result = list(scc.collection.find(json.loads(query)))

@Then('result would be empty')
def check_empty():
    assert [ ] == scc.find_result
