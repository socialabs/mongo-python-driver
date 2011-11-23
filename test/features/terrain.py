from test.test_connection import get_connection
from lettuce import world
world.db = get_connection().test
