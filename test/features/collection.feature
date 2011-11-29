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

Feature: Structure.Databases.Collections.Operations.Drop
    Drop operation deletes a collection as well as all of its indexes.

    Background:
        Given connection to "mongodb://localhost"
        And "query-test" database selected

    Scenario: Drop not empty collection
        When "tmp" collection selected
        And "tmp" collection truncated
        And following documents inserted
        | name     | age | dateOfVisit           |
        | Lillith  | 21  | 2010-10-12T00:00:00Z  |
        | Aubrey   | 36  | 2010-10-13T00:00:00Z  |
        | Cheyenne | 78  | 2010-10-11T00:00:00Z  |
        And "tmp" collection dropped
        And find documents by
        """
         {}
        """
        Then result would be empty
