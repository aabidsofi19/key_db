# import timeit
import key_db
import pytest
import time

db = None 

@pytest.fixture
def big_db(dummy_employee,file_path) :
    global db 
    if db is None :
        db = key_db.load(file_path)
        for i in range(1000_000) :
            db.set(f"Employee-{i}",dummy_employee)
    return db 

class PythonicDb() :

    def __init__(self) :

        self.db = {}

    def set(self,key,value) :
        self.db[key]= value 
    
    def get(self,key):
        return self.db[key]

@pytest.fixture
def db_python(dummy_employee) :
    
    db = PythonicDb()
    for i in range(1000_000) :
            db.set(f"Employee-{i}",dummy_employee)
    return db 






def  timeit(func,repeat=100) :
    
    total = []

    for _ in range(repeat) :
        start = time.perf_counter_ns()
        func()
        total.append((time.perf_counter_ns()-start))
    
    return (sum(total)/repeat)


def per_sec(time_taken_nanosecs) :

    return 1000000000 // time_taken_nanosecs

           
            

def test_performance(big_db,dummy_employee) : 
    
    insert_times = timeit(lambda : big_db.set("employee",dummy_employee))
    print("Insert Time :-" , insert_times , "nano seconds")
    print("Writes Per Second := ",per_sec(insert_times))
    
    get_times =  timeit(lambda : big_db.get("employee"))
    print("Read Time :-" , get_times , "nano seconds")
    print("Reads Per Second := ",per_sec(get_times))
 

def test_performance_python(db_python,dummy_employee) : 
    
    insert_times = timeit(lambda : db_python.set("employee",dummy_employee))
    print("Insert Time :-" , insert_times , "nano seconds")
    if insert_times > 0 :
        print("Writes Per Second := ",per_sec(insert_times))
    

    get_times =  timeit(lambda : db_python.get("employee"))
    print("Read Time :-" , get_times , "nano seconds")
    print("Reads Per Second := ",per_sec(get_times))
 

