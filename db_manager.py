import psycopg2
import config

class DBConnect:
    def __init__(self, host=config.DATABASES['HOST'], user=config.DATABASES['USER'], 
                dbname=config.DATABASES['DB_NAME'], password=config.DATABASES['PASSWORD'],
                sslmode=config.DATABASES['SSLMODE']):
        # Construct connection string
        self.conn_string = 'host={0} user={1} dbname={2} password={3}'.format(host, user, dbname, password, sslmode)
        try:
            self.conn = psycopg2.connect(self.conn_string)
            self.conn.autocommit = True
            self.cur = self.conn.cursor()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    
    def get_cursor(self):
        return self.cur

    def close(self):
        try:
            self.conn.commit()
            self.cur.close()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        finally:
            if self.conn is not None:
                self.conn.close()
    def get_con(self):

        return self.conn