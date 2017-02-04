import threading
import mysql.connector

# Allows multithreading when creating a connection to database and executing a ddl.
class connectionThread (threading.Thread):
    def __init__(self, threadID, config, ddl):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.config = config
        self.ddl = ddl

    def run(self):
        try:
            connection = mysql.connector.connect(**self.config)
            cursor = connection.cursor()
            cursor.execute(self.ddl)
            connection.close()
            cursor.close()
        except mysql.connector.Error as err:
            print(err.msg)
