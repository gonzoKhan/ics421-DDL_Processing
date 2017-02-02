import threading

# Allows multithreading when creating a connection to database and executing a ddl.
class connectionThread (threading.Thread):
    def __init__(self, threadID, config, ddl):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.config = config
        self.ddl = ddl

    def run(self)
        try:
            connection = mysql.connector.connect(**config)
            cursor = connection.cursor()
            cursor.execute(ddl)
            connection.close()
        except mysql.connector.Error as err:
            print(err.msg)

        cursor.close()
        connection.close()
