import key_db


def test_load_empty_db(file_path) :
    

    db = key_db.load(file_path)

    assert db 
   

def test_get_set(file_path,dummy_employee) :
    employee_dict = dummy_employee
    db = key_db.load(file_path)


    db.set("employee1", employee_dict )

    assert db.get("employee1") == employee_dict


def test_is_data_persisted(file_path,dummy_employee) :

    db = key_db.load(file_path)

    db.set("employee1" , dummy_employee)
    db.dump()

    db = key_db.load(file_path)

    assert db.get("employee1") == dummy_employee

    


