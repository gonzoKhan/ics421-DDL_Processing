import sys
import re
import mysql.connector

# Parses a line seperating it into a 3-tuple
def parseLine(line):
    try:
        m = re.match('^(\w+)\.(\w+)=(.*)$', line)
        return m.group(1,2,3)
    except AttributeError:
        return None

# Runs the ddl for the given connection in the config dictionary object.
def runSQL(config, ddl):
    try:
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        cursor.execute(ddl)
        connection.close()
    except mysql.connector.Error as err:
        print(err.msg)

    cursor.close()
    connection.close()

# Allows custom config and ddl files to be run as parameters.
# clusrcfg and ddlfile are the default if no parameters are entered.

if len(sys.argv) > 1:
    clustername = sys.argv[1]
else:
    clustername = 'clustercfg'
if len(sys.argv) > 2:
    ddlname = sys.argv[2]
else:
    ddlname = 'ddlfile'

# Opens the config and ddl files
cluster = open(clustername,'r')
ddl = open(ddlname,'r')

#Sets Variables from config
clusterconfig = cluster.readline()
nodes = re.match('^numnodes=(\d+)$', clusterconfig, flags=re.MULTILINE).group(1)
print ("nodes={0}".format(nodes))

node = [[0 for n in range(int(nodes)+1)] for m in range(5)]

for x in range(int(nodes)+1):
    currentline = cluster.readline()
    for y in range(4):
        if currentline == '\n':
            y = y-1

        else:
            tupled = parseLine(currentline)
            print (tupled)
            node[x][y] = tupled[2]
            currentline = cluster.readline()



# Connects to the mysql database
# connect = mysql.connector.connect(user='dbuser',
# password="jesus", host='127.0.0.1',
# database='test')
# connect.close()


#print (cluster.read() + ddl.read())
cluster.close()
ddl.close()
