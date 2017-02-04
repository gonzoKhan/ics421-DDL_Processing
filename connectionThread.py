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
            print("SUCCESS: THREAD{0} (database={1},hostname={2}): Sucessfully executed DDL".format(self.threadID, self.config['database'], self.config['host']))
        except mysql.connector.Error as err:
            print("FAILURE: THREAD{0} (database={1},hostname={2}): ".format(self.threadID, self.config['database'], self.config['host']) + err.msg)
