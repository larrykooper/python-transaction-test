import logging
import db_connection
import utils
from utils import rel_path

class Executive(object):

    def __init__(self):
        self.db_conn = db_connection.DbConnection.from_yaml(rel_path('config/database.yml'), 'larry')

    def run(self):
        print 'at the beginning of run'
        self.do_sql1()
        self.do_sql2()

    def do_sql1(self):
        print 'at the beginning of sql1'
        with self.db_conn.new_cursor() as cursor:
            cursor.execute_file(rel_path('sql/sql1.sql'))

    def do_sql2(self):
        print 'at the beginning of sql2'
        with self.db_conn.new_cursor() as cursor:
            cursor.execute_file(rel_path('sql/sql2.sql'))

def main():
    logging.basicConfig(level=logging.DEBUG)
    executive = Executive()
    executive.run()

if __name__ == '__main__':
    main()
