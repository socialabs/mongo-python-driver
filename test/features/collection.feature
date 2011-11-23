Feature: Collections
    Test Mongo collections

    Scenario: Rename collection
        Given a collection named test with 10 documents
        When I rename it to foo
        Then there are 10 documents in collection foo
        And documents in collection foo have x = 0,1,2,3,4,5,6,7,8,9
        And there are 0 documents in collection test