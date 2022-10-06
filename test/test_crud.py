import key_db

def test_load_empty_db(file_path) :
    
    db = key_db.load(file_path)
    assert db 
    db.close()


def test_get_set(db,dummy_employee) :
    
 
    employee_dict = dummy_employee
    db.set("employee1", employee_dict )

    assert db.get("employee1") == employee_dict
    assert not db.get("not_in_db" )

    
def test_remove(db,dummy_employee) :


    db.set("employee_rem",dummy_employee)
    db.remove("employee_rem")
    
    assert db.get("employee_rem") is None 



