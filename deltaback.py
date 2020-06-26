import mysql.connector as db
import json
import sys
from time import sleep
import re
import argparse

basedict = {}
deltadic = {}
sort = []
entry = []
finallist = []

adress = "95.97.78.84"
login = "rfscoutuser"
passw = "ahphae8eVeethu"
datab = "rfscout"

ap = argparse.ArgumentParser()
ap.add_argument('-t','--table',required=True)
args = vars(ap.parse_args())
mac_arg = str(args['table'])

connection = db.MySQLConnection(host=adress, user=login, passwd=passw, db=datab)

def baseline(connect, selectscan_id_comparein, mac_arg):
    sortlist = []
    basedictionary = {}
    entry = []
    if (selectscan_id_comparein == 0) or (selectscan_id_comparein == ""):
        sleep(5)
    else:
        cursor = connect.cursor(buffered= True)
        cursor.execute("select entry_id from scan where macaddress = %s ORDER BY entry_id DESC limit 1" % mac_arg)
        connect.commit()
        for count in cursor:
            mostrecent_id = count[0]
        cursor.execute("select scan_data, time_stamp from scan where entry_id = %s AND macaddress = %s" % (selectscan_id_comparein, mac_arg))
        for items in cursor:
            data, time = items
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
                basedictionary[freq] = dBm
                sortlist.append(freq)
        return mostrecent_id, selectscan_id_comparein, time, sortlist, basedictionary

def compare(connect, basedictionary,  mac_arg):
    deltadictionary = {}
    cursor.execute("SELECT scan_data, time_stamp from scan where macaddress = %s ORDER BY entry_id DESC LIMIT 1" % mac_arg)
    connect.commit()
    for items in cursor:
        data, time = items
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
            if freq in basedictionary:
                dbmcompare = basedictionary.get(freq,"none")
                calc = dBm-dbmcompare
                deltadictionary[freq] = calc
            else:
                deltadictionary[freq] = 0

    return deltadictionary

def sorter(sortlist, deltadictionary):
    final_list = []
    for index in sortlist:
        if index in deltadictionary:
            holddBm = deltadictionary.get(index,"none")
        else:
            holddBm = int(0)
        final_entry = [index, holddBm]
        final_list.append(final_entry)
    return final_list


config = connection.cursor(buffered= True)
cursor = connection.cursor(buffered= True)

FT = True

while True:
    config.execute("select deltalog_state, deltalog_scan from ScriptConfig where macaddress = %s" % mac_arg)
    connection.commit()
    for ScriptConfig in config:
        deltalog_state, deltalog_scan = ScriptConfig
        if deltalog_state == 1:
            if FT == True:
                mostrecent_id_base, selectscan_id_base, selectscan_time, sort, basedic = baseline(connection, deltalog_scan, mac_arg)
                FT = False
            #main loop
            elif FT == False:
                #if the selected id is different from the current one
                if deltalog_scan != selectscan_id_base:
                    mostrecent_id_base, selectscan_id_base, selectscan_time, sort, basedic = baseline(connection, deltalog_scan, mac_arg)
                else:
                    cursor.execute("select entry_id from scan where macaddress = %s ORDER BY entry_id DESC limit 1" % mac_arg)
                    connection.commit()
                    for compareval in cursor:
                        if compareval[0] > mostrecent_id_base:
                            mostrecent_id_base = compareval[0]
                            deltadic = compare(connection, basedic, mac_arg)
                            finallist = []
                            finallist = sorter(sort, deltadic)
                            print(finallist)
                            cursor.execute("select count(*) from delta where macaddress = %s" % mac_arg)
                            for updatecount in cursor:
                                connection.commit()
                                if updatecount[0] > 0:
                                    cursor.execute("Update delta set scan_data = '%s', base_time = '%s' where macaddress = %s" % (finallist, selectscan_time, mac_arg))
                                elif updatecount[0] == 0:
                                    cursor.execute("insert into delta (scan_data, base_time, macaddress) values ('%s', '%s', %s)" % (finallist, selectscan_time, mac_arg))
                                connection.commit()
        
        else:
            print("deltalog idle")
            sleep(10)
















