import sys
import re
import mysql.connector

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
currentline = cluster.readline()
currentline = currentline.rstrip()
splittemp = currentline.split('=')
nodes = splittemp[1]
print (nodes)

# Connects to the mysql database
connect = mysql.connector.connect(user='dbuser',
password="jesus", host='127.0.0.1',
database='test')
connect.close()


print (cluster.read() + ddl.read())
cluster.close()
ddl.close()
