# 🏃 runDB ![CI](https://github.com/lfrati/runDB/actions/workflows/tests.yml/badge.svg)

Have you ever looked at your computer and died inside a bit seeing thousands if not million of folders full of [JSON](https://docs.python.org/3/library/json.html) files with results of experiments?
If only storing them inside a nice SQL databse was as easy as json.dumps(). What if it could be that easy? After all python comes equipped with it's own nifty [sqlite3](https://docs.python.org/3/library/sqlite3.html).

# Dependencies
None. You are good to go :)

# Requirements
Only `python >= 3.10`

# Usage
Doc are WIP, but in the meanwhile [tests](https://github.com/lfrati/runDB/blob/main/tests/test_rundb.py) show everything you need:
```python
db_file = f"my_run.db" # maybe a better name than this? up to you.
# Let's log some hyper-params per run, and a simple steps/loss for the actual data
# This only creates the database file and the tables! 
create_db(db_file, info_cols={"init": str, "steps": int, "lr": float}, data_cols={"step": int, "loss": float})
```
Now you are ready to launch all the experiments you want and all you need to do is pass them the `db_file` (god bless SQLite):
```python
... in some other worker ...
# This creates a new entry in the INFO_TABLE and creates a unique (primary key) run_id
db = runDB(emptydb_file, {"init": "xavier", "lr": 0.001, "steps": 10})
...
for i in range(run["steps"]):
    # This creates a new row in the DATA_TABLE that containis the logged information and the run_id (foreign key)
    db.insert({"step": i, "loss": 1 / (i + 1.0)})
```
These functions are just wrappers around [sqlite3](https://docs.python.org/3/library/sqlite3.html), you can just query the underlying database as you see fit:
```python
    sql = """
        SELECT runid
        FROM INFO 
        WHERE init = :init AND lr = :lr AND steps = :steps
    """
    args = ({"init":"xavier", "lr":0.001, "steps":20})

    # Use sqlite3 directly
    with sqlite3.connect(db_file) as con:
      cur = con.execute(sql, args)
      results = cur.fetchall()
    con.close() # yup you still need to close it

    # or the provided utility, with some extra fancyness
    from rundb import query
    results = query(db_file, sql=sql, args=args, as_dict=True)
```
