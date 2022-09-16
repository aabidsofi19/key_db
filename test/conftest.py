import pytest  
import json
from pathlib import Path


@pytest.fixture
def file_path():

    FILE_PATH = "temp.db"
    
    yield FILE_PATH
     
    if Path(FILE_PATH).exists() :
        Path(FILE_PATH).unlink


@pytest.fixture
def test_db() :

    with open("test.db") as f :
        yield f

@pytest.fixture
def dummy_employee() :
 
    name = "John"
    projects = [ "Project1" , {"name":"eccommerce"}, ["rank1"]]

    return  {
            "name" : name ,
            "age": 10,
            "projects" : projects 
    }




