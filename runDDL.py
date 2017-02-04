import sys
import re
import mysql.connector
import io

# Parses a line seperating it into a 3-tuple
def parseLine(line):
    try:
        m = re.match('^(\w+)\.(\w+)=(.*)$', line)
        return m.group(1,2,3)
    except AttributeError:
        return None

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

# Sets Variables from config
clusterconfig = cluster.read()
print(clusterconfig)

# Grab the number of nodes then remove the line.
numnodes = re.search('^numnodes=(\d+)$', clusterconfig, flags=re.MULTILINE | re.IGNORECASE).group(1)
numnodes = int(numnodes)
clusterconfig = re.sub('^numnodes=(\d+)$', '', clusterconfig, count=1, flags=re.MULTILINE | re.IGNORECASE)

print("nodes={0}".format(numnodes))
print(clusterconfig)

c_driver = re.search('^catalog\.driver=(.*)$', clusterconfig, flags=re.MULTILINE | re.IGNORECASE).group(1)
clusterconfig = re.sub('^catalog\.driver=.*$', '', clusterconfig, count=1, flags=re.MULTILINE | re.IGNORECASE)

c_hostname = re.search('^catalog\.hostname=(.*)$', clusterconfig, flags=re.MULTILINE | re.IGNORECASE).group(1)
clusterconfig = re.sub('^catalog\.hostname=.*$', '', clusterconfig, count=1, flags=re.MULTILINE | re.IGNORECASE)

c_username = re.search('^catalog\.username=(.*)$', clusterconfig, flags=re.MULTILINE | re.IGNORECASE).group(1)
clusterconfig = re.sub('^catalog\.username=.*$', '', clusterconfig, count=1, flags=re.MULTILINE | re.IGNORECASE)

c_password = re.search('^catalog\.passwd=(.*)$', clusterconfig, flags=re.MULTILINE | re.IGNORECASE).group(1)
clusterconfig = re.sub('^catalog\.passwd=.*$', '', clusterconfig, count=1, flags=re.MULTILINE | re.IGNORECASE)

catalog_info = {
    'driver': c_driver,
    'hostname': c_hostname,
    'username': c_username,
    'password': c_password
}

print(catalog_info)
print(clusterconfig)

c_buffer = io.StringIO(clusterconfig)

nodes = [dict() for x in range(numnodes)]

for line in c_buffer:
    if line:
        parsed = parseLine(line)
        if parsed:
                print(parsed)



# for x in range(1,int(nodes)+1):
#     for y in range(0,5):
#         currentline = cluster.readline()
#         temp = parseLine(currentline)
#         if temp != None:
#             (name, att, val) = temp
#             node[x, att] = val

print (nodes)

# For loop that parses clusterconfig into an array of dictionary objects
# containing the keys:
#       nodename, driver, hostname, username, password

# node = [[0 for n in range(int(nodes)+1)] for m in range(5)]
#
# for x in range(int(nodes)+1):
#     currentline = cluster.readline()
#     for y in range(4):
#         if currentline == '\n':
#             print(' ')
#             #y = y-1
#
#         else:
#             tupled = parseLine(currentline) two key one value
#             print (tupled))
#             if tupled != '':
#                 print(x,y)
#
#                 node[x][y] = tupled[2]
#             currentline = cluster.readline()


# For loop that generates a dictionary object containing the parameters
# for a nodes connection then makes a connectionThread for each node.


# For loop that runs each connectionThread.


# Connects to the mysql database
# connect = mysql.connector.connect(user='dbuser',
# password="jesus", host='127.0.0.1',
# database='test')
# connect.close()


#print (cluster.read() + ddl.read())
cluster.close()
ddl.close()
