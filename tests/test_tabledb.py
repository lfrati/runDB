from multiprocessing import Process
from random import random
import sqlite3
import tempfile
from time import sleep

import pytest

from tabledb import tableDB


@pytest.fixture
def fulldb():
    """
    Create a test.db in a temporary folder and insert a few items into it.
    """
    folder = tempfile.mkdtemp()
    db_name = f"{folder}/test.db"
    db = tableDB(db_name)
    db.create("name", "lr", "acc")
    db.insertone({"name": "zio", "lr": 0.001, "acc": 0.98})
    db.insertone({"name": "pio", "lr": 0.002, "acc": 0.01})
    db.insertone({"name": "ciao", "lr": 0.3, "acc": 1.0})
    return db


@pytest.fixture
def emptydb():
    """
    Create an empty test.db in a temporary folder.
    """
    folder = tempfile.mkdtemp()
    db_name = f"{folder}/test.db"
    db = tableDB(db_name)
    return db


def test_create_insert_query(emptydb):
    entry = {"name": "zio", "lr": 0.001, "acc": 0.98}
    emptydb.create("name", "lr", "acc")  # NOTE: don't pass them as a list!
    emptydb.insertone(entry)
    results = emptydb.query("name", "zio")
    assert len(results) == 1
    retrieved = results[0]
    assert all([entry[key] == retrieved[key] for key in entry.keys()])


def test_insert_wrongkey(fulldb):
    with pytest.raises(KeyError):
        fulldb.insertone({"name": "casa", "MISSING": 0.001, "acc": 0.98})


def test_create_duplicate(fulldb):
    with pytest.raises(sqlite3.IntegrityError):
        fulldb.insertall(
            [
                {"name": "zio", "lr": 0.001, "acc": 0.98},
                {"name": "zio", "lr": 0.001, "acc": 0.98},
            ]
        )


def test_delete(fulldb):
    results = fulldb.query("name", "zio")
    assert len(results) == 1

    fulldb.deleteone("name", "zio")

    results = fulldb.query("name", "zio")
    assert len(results) == 0


def test_concurr_write(emptydb):
    emptydb.create("name", "lr", "acc")  # NOTE: don't pass them as a list!

    def work(db, entry):
        t = random()
        sleep(t)
        db.insertone(entry)

    import resource

    nfiles, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    # each worker uses 2 files (1 to subprocess, 1 to open db), let's be safe
    N = nfiles // 3

    procs = []
    entries = [
        {"name": f"worker_{i}", "lr": random(), "acc": random()} for i in range(N)
    ]

    for entry in entries:
        proc = Process(target=work, args=(emptydb, entry))
        procs.append(proc)
        proc.start()

    # complete the processes
    for proc in procs:
        proc.join()

    for entry in entries:
        results = emptydb.query("name", entry["name"])
        assert len(results) == 1
        retrieved = results[0]
        assert all(
            [entry[key] == retrieved[key] for key in entry.keys()]
        ), f"ERROR: {entry} != {retrieved}"
