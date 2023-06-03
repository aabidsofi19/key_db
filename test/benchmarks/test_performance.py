# import timeit
from contextlib import contextmanager
import layer_db
import pytest
import time
import pickledb
from pathlib import Path


@pytest.fixture
def big_db():
    db = layer_db.load("test_big.db")

    if not Path("test_big.db").exists():
        print("intializing big key db")
        for i in range(1000):
            db.set(f"Employee-{i}", "hello" * 100)

    yield db
    db.close()


@pytest.fixture
def big_pickle_db(dummy_employee):
    pickle_db = pickledb.load("test_pickele.db", True)
    if not Path("test_pickele.db").exists():
        print("intializing big pickle db")
        for i in range(1000):
            pickle_db.set(f"Employee-{i}", dummy_employee)
    yield pickle_db


class PythonicDb:
    def __init__(self):
        self.db = {}

    def set(self, key, value):
        self.db[key] = value

    def get(self, key):
        return self.db[key]


@pytest.fixture
def db_python(dummy_employee):
    db = PythonicDb()
    for i in range(1000_000):
        db.set(f"Employee-{i}", dummy_employee)
    return db


# @pytest.mark.skip(reason="Takes long time")
def test_performance_on_big_layer_db(big_db, dummy_employee, benchmark):
    benchmark(big_db.set, "employee", dummy_employee)


def test_performance_on_big_pickle_db(big_pickle_db, dummy_employee, benchmark):
    benchmark(big_pickle_db.set, "employee", dummy_employee)


def test_empty_db(file_path, dummy_employee, benchmark):
    db = layer_db.load(file_path)
    benchmark(db.set, "hello", dummy_employee)
    db.close()


def test_pickle_db_small(dummy_employee, benchmark):
    db = pickledb.load("pickled.db", True)
    benchmark(db.set, "hello", dummy_employee)


def test_performance_python(db_python, dummy_employee, benchmark):
    benchmark(db_python.set, "employee", dummy_employee)
