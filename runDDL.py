import sys
import re
import io
from connectionThread import connectionThread

# Parses a line seperating it into a 3-tuple
def parseLine(line):
    try:
        m = re.match('^\w+(\d+)\.(\w+)=(.*)$', line)
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
ddlfile = open(ddlname,'r')

# Get strings for clusterconfig and ddl
clusterconfig = cluster.read()
ddl = ddlfile.read()

# Grab the number of nodes then remove the line.
numnodes = re.search('^numnodes=(\d+)$', clusterconfig, flags=re.MULTILINE | re.IGNORECASE).group(1)
numnodes = int(numnodes)
clusterconfig = re.sub('^numnodes=(\d+)$', '', clusterconfig, count=1, flags=re.MULTILINE | re.IGNORECASE)

# Grab the config for the catalog
c_driver = re.search('^catalog\.driver=(.*)$', clusterconfig, flags=re.MULTILINE | re.IGNORECASE).group(1)
clusterconfig = re.sub('^catalog\.driver=.*$', '', clusterconfig, count=1, flags=re.MULTILINE | re.IGNORECASE)

c_hostname = re.search('^catalog\.hostname=(.*)$', clusterconfig, flags=re.MULTILINE | re.IGNORECASE).group(1)
clusterconfig = re.sub('^catalog\.hostname=.*$', '', clusterconfig, count=1, flags=re.MULTILINE | re.IGNORECASE)

c_username = re.search('^catalog\.username=(.*)$', clusterconfig, flags=re.MULTILINE | re.IGNORECASE).group(1)
clusterconfig = re.sub('^catalog\.username=.*$', '', clusterconfig, count=1, flags=re.MULTILINE | re.IGNORECASE)

c_password = re.search('^catalog\.passwd=(.*)$', clusterconfig, flags=re.MULTILINE | re.IGNORECASE).group(1)
clusterconfig = re.sub('^catalog\.passwd=.*$', '', clusterconfig, count=1, flags=re.MULTILINE | re.IGNORECASE)

# Parse hostname into address and database
hostmatch = re.search('^.*//([\.\d]+):(\d+)/(.*)$', c_hostname, flags=re.IGNORECASE)
c_address = hostmatch.group(1)
c_port = hostmatch.group(2)
c_database = hostmatch.group(3)

catalog_info = {
    'driver': c_driver,
    'full_hostname': c_hostname,
    'hostname': c_address,
    'username': c_username,
    'password': c_password,
    'port': c_port,
    'database': c_database
}

# Use StrinIO to read string line by line.
c_buffer = io.StringIO(clusterconfig)

# List of dictionary objects that will hold the parsed config data.
nodes = [dict() for x in range(numnodes)]

# Read each line into the dictionary "nodes"
for line in c_buffer:
    if line:
        parsed = parseLine(line)
        if parsed and len(parsed) == 3:
            (num, key, value) = parsed
            try:
                nodes[int(num)-1][key] = value
            except:
                print("Error: numnodes or node numbers in config are incorrect.")


for idnum in range(len(nodes)):
    if 'hostname' in nodes[idnum]:
        hostmatch = re.search('^.*//([\.\d]+):(\d+)/(.*)$', nodes[idnum]['hostname'], flags=re.IGNORECASE)
        nodes[idnum]['address'] = hostmatch.group(1)
        nodes[idnum]['port'] = hostmatch.group(2)
        nodes[idnum]['database'] = hostmatch.group(3)

# For loop that generates a dictionary object containing the parameters
# for a nodes connection then makes a connectionThread for each node.
threads = list()
try:
    for idnum in range(len(nodes)):
        config = {
            'user': nodes[idnum]['username'],
            'password': nodes[idnum]['passwd'],
            'host': nodes[idnum]['address'],
            'port': nodes[idnum]['port'],
            'database': nodes[idnum]['database']
        }
        threads.insert( -1, connectionThread(idnum+1, config, ddl, nodes[idnum], catalog_info) )

    # For loop that runs each connectionThread.
    for conn in threads:
        conn.run()
except KeyError as err:
    print('INVALID FORMAT IN CLUSTERCONFIG: Expected ' + str(err))
    print('FAILED TO EXECUTE DDL')

cluster.close()
ddlfile.close()
