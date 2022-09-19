# import timeit
import key_db
import pytest
import time
import random
import pickledb

db = None 

@pytest.fixture
def big_db(dummy_employee,file_path) :
    global db 
    if db is None :
        db = key_db.load(file_path)
        for i in range(1000_000) :
            db.set(f"Employee-{i}",dummy_employee)
    yield db
    db.close()

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

           
            

def test_performance_on_big_db(big_db,dummy_employee,benchmark) : 
    # 
    # insert_times = timeit(lambda : big_db.set("employee",dummy_employee))
    # print("Insert Time :-" , insert_times , "nano seconds")
    # print("Writes Per Second := ",per_sec(insert_times))
    # 
    # get_times =  timeit(lambda : big_db.get("employee"))
    # print("Read Time :-" , get_times , "nano seconds")
    # print("Reads Per Second := ",per_sec(get_times))
    

    benchmark(big_db.set , "employee" , dummy_employee )


def test_small_db(file_path,dummy_employee,benchmark) :
    db = key_db.load("testy.db")
    benchmark(db.set,"hello",dummy_employee)


def test_small_db_small_value(dummy_employee,benchmark) :
    db = key_db.load("testy.db")
    benchmark(db.set,"hello","world")


def test_pickle_db(dummy_employee,benchmark) :
    db = pickledb.load("pickled.db",True)
    benchmark(db.set,"hello",dummy_employee)

#
#
# def test_insert_performance(dummy_employee) : 
#     big_db = key_db.load(f"test.db")
#     insert_times = timeit(lambda : big_db.set(f"employee-{random.random()}",dummy_employee),repeat=1000)
#     print("Insert Time :-" , insert_times , "nano seconds")
#     print("Writes Per Second := ",per_sec(insert_times))
#    
#


def test_performance_python(db_python,dummy_employee ,benchmark ) : 
    
 #    insert_times = timeit(lambda : db_python.set("employee",dummy_employee))
 #    print("Insert Time :-" , insert_times , "nano seconds")
 #    if insert_times > 0 :
 #        print("Writes Per Second := ",per_sec(insert_times))
 #    
 #
 #    get_times =  timeit(lambda : db_python.get("employee"))
 #    print("Read Time :-" , get_times , "nano seconds")
 #    print("Reads Per Second := ",per_sec(get_times))
 # 
    benchmark(db_python.set , "employee" , dummy_employee)
