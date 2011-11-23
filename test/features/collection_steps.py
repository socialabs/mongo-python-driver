# Copyright 2009-2010 10gen, Inc.
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

from lettuce import step, world, before, after

def drop_collections(step):
    db = world.db
    db.drop_collection('test')
    db.drop_collection('foo')

before.each_scenario(drop_collections)
after.each_scenario(drop_collections)

@step('a collection named (.*) with (\d+) documents$')
def collection_with_docs(step, name, n_docs):
    world.collection_name = name
    db = world.db
    collection = db[name]
    for i in range(10):
        collection.insert({"x": i})

@step('I rename it to (.*)')
def rename(step, new_name):
    old_name = world.collection_name
    world.db[old_name].rename(new_name)

@step('there are (\d+) documents in collection (.*)$')
def count_docs(step, expected_count, collection_name):
    count = world.db[collection_name].count()
    assert count == int(expected_count), \
        "Got %s" % count

@step('documents in collection (.*) have (.*) = ((?:\d+,)+?\d+)$')
def pick(step, collection_name, key_name, value_list):
    expected = set(int(i.strip()) for i in value_list.split(','))
    actual = set(
        doc[key_name] for doc in world.db[collection_name].find()
    )

    print 'actual', actual
    print 'expected', expected

    assert expected == actual
