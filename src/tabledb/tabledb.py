import sqlite3

# import pandas as pd


class tableDB:
    def __init__(self, file):
        self.db = file
        self.con = sqlite3.connect(file)
        self.cur = self.con.cursor()
        self.table = "data"

        try:
            self.cur.execute(f"SELECT * FROM {self.table}")
            self.columns = [cols[0] for cols in self.cur.description]
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
        self.cur.execute(self.cmd)
        self.con.commit()

    def insertall(self, entries):
        for entry in entries:
            self.insertone(entry)

    def insertone(self, entry):
        placeholders = ",".join(["?" for _ in self.columns])
        data = [entry[col] for col in self.columns]
        self.cur.execute(f"INSERT INTO {self.table} VALUES({placeholders})", data)
        self.con.commit()

    def deleteall(self, col, values):
        for val in values:
            self.deleteone(col, val)

    def deleteone(self, col, val):
        cmd = f"DELETE FROM {self.table} WHERE {col}='{val}';"
        self.cur.execute(cmd)
        self.con.commit()

    def query(self, col, name):
        assert (
            len(self.columns) > 0
        ), "Error: table has not been initialized yet. Please use: create(table, *columns) first."
        self.cur.execute(f"SELECT * FROM {self.table} WHERE {col} = ?", (name,))
        results = self.cur.fetchall()
        if len(results) > 0:
            return [{k: v for k, v in zip(self.columns, result)} for result in results]
        return []

    # def to_df(self):
    #     self.cur.execute(f"SELECT * FROM {self.table}")
    #     rows = self.cur.fetchall()
    #     df = pd.DataFrame(columns=self.columns, data=rows)
    #     return df


#
# db = tableDB("test.db")
#
# db.create("name", "lr", "acc")
#
# db.insertone({"name": "zio", "lr": 0.001, "acc": 0.98})
#
# try:
#     db.insertall(
#         [
#             {"name": "zio", "lr": 0.001, "acc": 0.98},
#             {"name": "zio", "lr": 0.001, "acc": 0.98},
#         ]
#     )
# except sqlite3.IntegrityError:
#     print("CAN'T INSERT MULTIPLE ZIOs")
#
# try:
#     db.insertall(
#         [
#             {"name": "zio", "lr": 0.001, "acc": 0.98},
#             {"name": "zio", "lr": 0.001, "acc": 0.98},
#         ]
#     )
# except sqlite3.IntegrityError:
#     print("CAN'T INSERT MULTIPLE ZIOs")
#
# print(db.query("name", "zio"))
#
# print(db.to_df())
