import pytest  
import key_db
from pathlib import Path
from contextlib import contextmanager

@pytest.fixture
def file_path():
    
    FILE_PATH = "test_dummy.db"
    if Path(FILE_PATH).exists() :
        Path(FILE_PATH).unlink()

    
    yield FILE_PATH
     
    if Path(FILE_PATH).exists() :
        Path(FILE_PATH).unlink()


@pytest.fixture
def db(file_path) :
    db = key_db.load(file_path)
    yield db 
    db.close()


@contextmanager
def get_db(file_path) :
    db = key_db.load(file_path) 
    yield db
    db.close()


@pytest.fixture
def dummy_employee() :
 
    name = "John"
    projects = [ "Project1" , {"name":"eccommerce"}, ["rank1"]]

    return  {
            "name" : name ,
            "age": 10,
            "projects" : projects 
    }

