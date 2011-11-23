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

Feature: Collections
    Test Mongo collections

    Scenario: Rename collection
        Given a collection named test with 10 documents
        When I rename it to foo
        Then there are 10 documents in collection foo
        And documents in collection foo have x = 0,1,2,3,4,5,6,7,8,9
        And there are 0 documents in collection test