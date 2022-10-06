from conftest import get_db

    
def test_set(file_path)  :
    
    with get_db(file_path) as db :
        db.set("test","test") 
    
    with get_db(file_path) as db :
        assert db.get("test") == "test"


def test_remove(file_path) :
    with get_db(file_path ) as db :
        db.set("test","test") 
        db.remove("test") 
    

    with get_db(file_path) as db :
        v = db.get("test")
        assert v is None



