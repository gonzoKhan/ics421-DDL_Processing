import sys
import mysql.connector

if (len(sys.argv) > 1)
    sys.argv[2]
connect = mysql.connector.connect(user='dbuser',
password="jesus", host='127.0.0.1',
database='test')

connect.close()
cluster = open('clustercfg','r')
ddl = open('ddlfile','r')
print (cluster.read() + ddl.read())
