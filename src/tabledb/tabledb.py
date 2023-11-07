import sqlite3


class tableDB:
    def __init__(self, file):
        self.db_file = file
        self.table = "data"

        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            try:
                cur.execute(f"SELECT * FROM {self.table}")
                _ = [cols[0] for cols in cur.description]
            except sqlite3.OperationalError:
                self.columns = []

    def create(self, *columns):
        """
        The first column is assumed to be the primary key.
        """
        assert len(self.columns) == 0, f"Error: the table has been initialized already"
        self.columns = columns
        pr_key = f"PRIMARY KEY ({self.columns[0]})"
        self.cmd = (
            f"CREATE TABLE IF NOT EXISTS {self.table}({', '.join(columns)}, {pr_key})"
        )
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute(self.cmd)
            con.commit()

    def insertall(self, entries):
        for entry in entries:
            self.insertone(entry)

    def insertone(self, entry):
        placeholders = ",".join(["?" for _ in self.columns])
        data = [entry[col] for col in self.columns]
        cmd = f"INSERT INTO {self.table} VALUES({placeholders})"
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            result = cur.execute(cmd, data)
            return result

    def deleteall(self, col, values):
        for val in values:
            self.deleteone(col, val)

    def deleteone(self, col, val):
        cmd = f"DELETE FROM {self.table} WHERE {col}='{val}';"
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute(cmd)
            con.commit()

    def query(self, col, name):
        assert (
            len(self.columns) > 0
        ), "Error: table has not been initialized yet. Please use: create(table, *columns) first."

        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute(f"SELECT * FROM {self.table} WHERE {col} = ?", (name,))
            results = cur.fetchall()
            return [{k: v for k, v in zip(self.columns, result)} for result in results]
