import mysql.connector as db
import json
import sys
from time import sleep
import re
import argparse


peakdic = {}
sort = []
entry = []
finallist = []

adress = "95.97.78.84"
login = "rfscoutuser"
passw = "ahphae8eVeethu"
datab = "rfscout"

connection = db.MySQLConnection(host=adress, user=login, passwd=passw, db=datab)
connection3 = db.MySQLConnection(host=adress, user=login, passwd=passw, db=datab)
cursor = connection.cursor(buffered= True) #rowcount
cursor2 = connection.cursor(buffered= True) #Peakdatabase
cursor3 = connection3.cursor(buffered= True) #data Database
cursor4 = connection.cursor(buffered= True) #Switch

cursor.execute("select count(*) from scan where macaddress = 202481592263953")
cursor3.execute("CREATE TABLE IF NOT EXISTS log (entry_id INT NOT NULL AUTO_INCREMENT, macaddress varchar(40) NOT NULL, frequency varchar(20), difference varchar(20), time_stamp varchar(50), PRIMARY KEY(entry_id))")
cursor2.execute("SELECT scan_data, time_stamp, start_freq, stop_freq from scan where macaddress = 202481592263953 ORDER BY entry_id DESC LIMIT 1")

#Setting the initial scan
for rowcount in cursor:
    rowcount = str(rowcount)
    basecount = int(rowcount[1:-2])
    for rows in cursor2:
        data = (rows[0])
        data = str(data[1:-1])
        length = data.count('[')
        entry[0:length] = data.split('[')
        del entry[0]
        for index in range(len(entry)):
            value = entry[index]
            entry[index] = value[:-3]
            freq, dBm = entry[index].split(',')
            freq = float(freq)
            dBm = float(dBm[1:])
            ###base lists
            peakdic[freq] = dBm
            sort.append(freq)

while True:
    cursor.execute("select count(*) from scan where macaddress = 202481592263953")
    sleep(2)
    for rowcount in cursor:
        connection.commit()
        comparecount = rowcount[0]
        if comparecount > basecount:
            basecount = comparecount
            cursor2.execute("SELECT scan_data, time_stamp, start_freq, stop_freq from scan where macaddress = 202481592263953 ORDER BY entry_id DESC LIMIT 1" )
            for rows in cursor2:
                time = (rows[1])
                data = (rows[0])
                data = str(data[1:-1])
                length = data.count('[')
                entry[0:length] = data.split('[')
                del entry[0]
                for index in range(len(entry)):
                    value = entry[index]
                    entry[index] = value[:-3]
                    freq, dBm = entry[index].split(',')
                    freq = float(freq)
                    dBm = float(dBm[1:])
                    if freq in peakdic:
                        dbmcompare = peakdic.get(freq,"none")
                        calc = dBm-dbmcompare
                        peakdic[freq] = dBm
                        if (calc > 30.0) or (calc < -30.0):
                            print ("from: %s to %s" % (dbmcompare, dBm))
                            cursor3.execute("INSERT INTO log (frequency, difference, time_stamp, macaddress) VALUES ('%s', '%s', '%s', 202481592263953)" % (freq, calc, time))
                            connection3.commit()
                
                    else:
                        peakdic[freq] = dBm













