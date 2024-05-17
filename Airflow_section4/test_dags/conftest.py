import pytest
from airflow.models import DagBag

# using pytest to define the fixture functions
# make them available across multiple test files

# fixture functions => fuctions which will run before each test function to applied
# feed some data to tests, ex) database connections, URL
# can attach fixture function to various test commonly 

@pytest.fixture(scope="session")
def dagbag():
    return DagBag()
# dagbag() will executed when dagbag created
# return DagBag() => returning DAGs from the folder dags  