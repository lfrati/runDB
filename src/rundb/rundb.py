import sqlite3
from pathlib import Path
from contextlib import closing

INFO_TABLE = "INFO"
DATA_TABLE = "DATA"


def table_exists(con, name):
    # CHECK IF TABLE EXISTS
    stmnt = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{name}';"
    cursor = con.execute(stmnt)
    return len(cursor.fetchall()) > 0


def get_columns(
    con: sqlite3.Connection, table: str, info: list[str] = ["name"]
) -> list[str]:
    # https://www.sqlite.org/pragma.html#pragma_table_info
    # This pragma returns one row for each normal column in the named table. Columns in the result set include:
    # 0 - "cid"?
    # 1 - "name" (its name);
    # 2 - "type" (data type if given, else '');
    # 3 - "notnull" (whether or not the column can be NULL);
    # 4 - "dflt_value" (the default value for the column);
    # 5 - "pk" (either zero for columns that are not part of the primary key, or the 1-based index of the column within the primary key).
    info2field = {"name": 1, "type": 2, "notnull": 3, "dflt_value": 4, "pk": 5}
    cursor = con.execute(f"PRAGMA table_info({table});")
    columns = cursor.fetchall()[1:]
    return [
        " ".join(col[info2field[x]] for x in info) for col in columns if col[-1] == 0
    ]


def are_same(l1, l2):
    if len(l1) != len(l2):
        return False
    for i, j in zip(l1, l2):
        if i != j:
            return False
    return True


def validate_cols(con, cols, table):
    existing_cols = get_columns(con, table, ["name", "type"])
    assert are_same(
        existing_cols, cols
    ), f"ERROR: {table} exists and {existing_cols=} != {cols=}"


# FROM: https://docs.python.org/3/library/sqlite3.html#how-to-create-and-use-row-factories
def dict_factory(cursor, row):
    fields = [column[0] for column in cursor.description]
    return {key: value for key, value in zip(fields, row)}


##############################################################
#                      EXPORTED                              #
##############################################################


# https://docs.python.org/3/library/sqlite3.html#sqlite-and-python-types
PY2SQL_TYPES = {None: "NULL", int: "INTEGER", float: "REAL", str: "TEXT", bytes: "BLOB"}
SQL2PY_TYPES = {val: key for key, val in PY2SQL_TYPES.items()}


def create_db(file, info_cols, data_cols, force=False):
    """
    Creates and initializes a sqlite3 database to log experiments results.
    The database will contain 2 tables:
        1. contains INFO about the run, for example hyper-parameters used.
        2. contains DATA from the run, for example steps/losses pairs.

    Entries in the DATA table contain a FOREIGN key runid that connectes them
    to the relevant INFO. Deletion is set to CASCADE so that deleting a runid
    removes all the associated entries from the DATA table.
    """
    file = Path(file)
    if file.exists():
        if force:
            print(f"bye bye {file}")
            Path(file).unlink()
        else:
            print(f"{file=} exists.")
            return

    # NOTE: below here FILE doesn't exists (if it existed it has been deleted)

    with closing(sqlite3.connect(file)) as con:
        # GENERATE INFO TABLE
        base = ["runid INTEGER PRIMARY KEY AUTOINCREMENT"]
        info_cols = [
            f"{name} {PY2SQL_TYPES[_type]} NOT NULL"
            for name, _type in info_cols.items()
        ]
        meta_decl = "(" + ", ".join(base + info_cols) + ")"
        con.execute(f"CREATE TABLE {INFO_TABLE} {meta_decl}")

        # GENERATE DATA TABLE
        base = ["runid INTEGER"]
        constrain = [
            f"FOREIGN KEY(runid) REFERENCES {INFO_TABLE}(runid) ON DELETE CASCADE"
        ]
        data_cols = [
            f"{name} {PY2SQL_TYPES[_type]} NOT NULL"
            for name, _type in data_cols.items()
        ]
        run_decl = "(" + ", ".join(base + data_cols + constrain) + ")"
        con.execute(f"CREATE TABLE {DATA_TABLE} {run_decl}")
        con.commit()


def delete_run(db_file: str, id: tuple) -> None:
    """
    Delete a single run and all the associated data (see ON DELETE CASCADE).
    """
    with closing(sqlite3.connect(db_file)) as con:
        cmd = f"DELETE FROM {INFO_TABLE} WHERE {INFO_TABLE}.runid = ?"
        con.execute(cmd, id)
        con.commit()


def query(
    db_file: str,
    sql: str,
    args: tuple = (),
    single: bool = False,
    as_dict: bool = False,
) -> list[tuple | dict]:
    """
    Utility to run a 'sql' command in 'db_file' without too much boilerplate.

    'as_dict=True' returns the results as dicts where keys are column names
    instead of lists of tuples.
    """
    with closing(sqlite3.connect(db_file)) as con:
        if as_dict:
            con.row_factory = dict_factory
        cur = con.execute(sql, args)
        if single:
            result = cur.fetchone()
        else:
            result = cur.fetchall()
        return result


class runDB:
    def __init__(self, file, run_info):
        self.db_file = file
        self.run_id = None

        assert Path(file).exists(), f"Database {self.db_file} has not been created yet."

        with closing(sqlite3.connect(self.db_file)) as con:
            assert table_exists(con, INFO_TABLE)
            assert table_exists(con, DATA_TABLE)

            self.data_cols = get_columns(con, DATA_TABLE)
            self.info_cols = get_columns(con, INFO_TABLE)

            run_placeholders = ", ".join([f":{key}" for key in self.data_cols])
            self.run_cmd = (
                f"INSERT INTO {DATA_TABLE} VALUES(:runid, {run_placeholders})"
            )
            meta_placeholders = ", ".join([f":{key}" for key in self.info_cols])
            self.create_run_cmd = (
                f"INSERT INTO {INFO_TABLE} VALUES(null, {meta_placeholders})"
            )

            # CREATE RUN
            self.run_id = con.execute(self.create_run_cmd, run_info).lastrowid
            con.commit()

        assert self.run_id != None, "Run creation failed."

    def __repr__(self):
        name = self.__class__.__name__
        return f"{name}({INFO_TABLE}={self.info_cols}, {DATA_TABLE}={self.data_cols})"

    def insert(self, entry):
        # SQLlite checks if keys are missing when using the :keyname format
        # but it doesn't check if there are extra ones. We know what should
        # be in the DATA table so we add a check before insert.
        assert entry.keys() == set(
            self.data_cols
        ), f"ERROR: wrong keys. Expected: {self.data_cols}, got {entry.keys()}"
        args = {"runid": self.run_id}
        args.update(entry)
        with closing(sqlite3.connect(self.db_file)) as con:
            con.execute(self.run_cmd, args)
            con.commit()


__all__ = ["runDB", "create_db", "query", "delete_run"]
