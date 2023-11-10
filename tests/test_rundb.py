from multiprocessing import Process
from random import random
import tempfile
from time import sleep

import pytest

from rundb import *

# import sqlite3

# INFO_COLS = ["init TEXT", "lr FLOAT", "steps INTEGER"]
# DATA_COLS = ["step INTEGER", "loss FLOAT"]

INFO_COLS = {"init": str, "steps": int, "lr": float}
DATA_COLS = {"step": int, "loss": float}

fake_runs = [
    {"init": "xavier", "lr": 0.001, "steps": 10},
    {"init": "kaiming", "lr": 0.001, "steps": 10},
    {"init": "xavier", "lr": 0.001, "steps": 20},
    {"init": "kaiming", "lr": 0.1, "steps": 10},
]


@pytest.fixture
def fulldb_file():
    """
    Create a test.db in a temporary folder and insert a few items into it.
    """
    folder = tempfile.mkdtemp()
    db_file = f"{folder}/test.db"
    create_db(db_file, info_cols=INFO_COLS, data_cols=DATA_COLS)

    for run in fake_runs:
        db = runDB(db_file, run)

        for i in range(run["steps"]):
            db.insert({"step": i, "loss": 1 / (i + 1.0)})

    return db_file


@pytest.fixture
def emptydb_file():
    """
    Create an empty test.db in a temporary folder.
    """
    folder = tempfile.mkdtemp()
    db_file = f"{folder}/test.db"
    create_db(db_file, info_cols=INFO_COLS, data_cols=DATA_COLS)
    return db_file


def test_insert(emptydb_file):
    db = runDB(emptydb_file, {"init": "xavier", "lr": 0.001, "steps": 10})
    db.insert({"step": 0, "loss": 0.98})


def test_fail_insert_wrong_field(emptydb_file):
    db = runDB(emptydb_file, {"init": "xavier", "lr": 0.001, "steps": 10})
    with pytest.raises(AssertionError):
        db.insert({"step": 0, "MISSING": 0.001, "loss": 0.98})


def test_fail_insert_missing_field(emptydb_file):
    db = runDB(emptydb_file, {"init": "xavier", "lr": 0.001, "steps": 10})
    # # it would a ProgrammingError if we didn't check the keys ourselves
    # with pytest.raises(sqlite3.ProgrammingError):
    with pytest.raises(AssertionError):
        db.insert({"step": 0})


def test_delete_run(fulldb_file):
    sql = """
        SELECT INFO.runid
        FROM INFO
        WHERE INFO.init = 'kaiming'
    """
    runids = query(fulldb_file, sql)

    for runid in runids:
        delete_run(fulldb_file, runid)

    after_del_runids = [val[0] for val in query(fulldb_file, sql=sql)]
    assert len(after_del_runids) == 0


def test_concurr_write(emptydb_file):
    def work(db_file, run):
        db = runDB(db_file, run)
        for i in range(run["steps"]):
            sleep(random() / 10)
            db.insert({"step": i, "loss": 1 / (i + 1.0)})

    procs = []

    for run in fake_runs:
        proc = Process(target=work, args=(emptydb_file, run))
        procs.append(proc)
        proc.start()

    # complete the processes
    for proc in procs:
        proc.join()

    for run in fake_runs:
        sql = """
            SELECT runid
            FROM INFO 
            WHERE init = ? AND lr = ? AND steps = ?
        """
        args = (run["init"], run["lr"], run["steps"])
        results = query(emptydb_file, sql=sql, args=args)
        runids = [val[0] for val in results]

        assert len(runids) == 1, runids

        sql = """
            SELECT 
                step, 
                loss
            FROM DATA 
            JOIN INFO ON DATA.runid = INFO.runid
            WHERE INFO.runid = ?
        """
        results = query(emptydb_file, sql=sql, args=runids)
        for res in results:
            print(res)
        print()
