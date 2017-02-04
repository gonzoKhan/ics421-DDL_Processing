import threading
import mysql.connector
import er

# Allows multithreading when creating a connection to database and executing a ddl.
class connectionThread (threading.Thread):
    def __init__(self, threadID, config, ddl, driver, catalog_info):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.config = config
        self.ddl = ddl
        self.driver = driver
        self.catalog_info = catalog_info

    def run(self):
        try:
            connection = mysql.connector.connect(**self.config)
            cursor = connection.cursor()
            cursor.execute(self.ddl)
            connection.close()
            cursor.close()
            __updateCatalog()
            print("SUCCESS: THREAD{0} (database={1},hostname={2}): Sucessfully executed DDL".format(self.threadID, self.config['database'], self.config['host']))
        except mysql.connector.Error as err:
            print("FAILURE: THREAD{0} (database={1},hostname={2}): ".format(self.threadID, self.config['database'], self.config['host']) + err.msg)

    def __updateCatalog(self):
        crt_table = (
            "CREATE TABLE "
            "DTABLES(tname char(32), "
            "nodedriver char(64), "
            "nodeurl char(128), "
            "nodeuser char(16), "
            "nodepasswd char(16), "
            "partmtd int, "
            "nodeid int, "
            "partcol char(32), "
            "partparam1 char(32), "
            "partparam2 char(32))"
        )

        try:
            connection = mysql.connector.connect(
                user = self.catalog_info['username'],
                password = self.catalog_info['password'],
                host = self.catalog_info['hostname'],
                database = 'catalog'
            )
            cursor = connect.cursor()
            # Attempt to create table if it doesn't exist.
            try:
                cursor.exectue(crt_table)
            except:
                pass

            try:
                tname = er.search("table (\w+)\(", ddl, flags=re.IGNORECASE).group(1)
                nodedriver = self.driver
                nodeurl = self.config['host'] + "/" + self.config['database']
                nodeuser = self.config['user']
                nodepasswd = self.config['password']
                nodeid = self.threadID
                s = (
                    "INSERT INTO DTABLES("
                    "(tname, nodedriver, nodeurl, nodeuser, nodepasswd, "
                    "partmtd, nodeid, partcol, partparam1, partparam2) "
                    "VALUES ({0}, {1}, {2}, {3}, {4}, NULL, {5}, NULL, NULL, NULL)"
                )
                cursor.execute(s.format(tname, nodedriver, nodeurl, nodeuseer, nodepasswd, nodeid))
            cursor.close()
            connection.close()
        except mysql.connector.Error as err:
            print(err)
            exit(1)
