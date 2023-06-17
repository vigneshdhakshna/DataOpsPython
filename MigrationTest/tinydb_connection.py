from tinydb import TinyDB

class TinyDBConnection:
    def __init__(self, db_file):
        self.db_file = db_file
        self.db = None

    def connect(self):
        self.db = TinyDB(self.db_file)

    def close(self):
        if self.db:
            self.db.close()

    def query_all(self, table_name):
        table = self.db.table(table_name)
        return table.all()
