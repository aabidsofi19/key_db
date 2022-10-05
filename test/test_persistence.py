import key_db 
from contextlib import contextmanager

@contextmanager
def get_db(file_path) :
    db = key_db.load(file_path) 
    yield db
    db.close()
    
def test_set(file_path)  :
    
    db = key_db.load(file_path)
    db.set("test","test") 
    db.close()

    db = key_db.load(file_path)
    v = db.get("test")
    assert v == "test"


def test_remove(file_path) :
    with get_db(file_path ) as db :
        db.set("test","test") 
        db.remove("test") 
    
    with get_db(file_path) as db :
        v = db.get("test")
        assert v is None



