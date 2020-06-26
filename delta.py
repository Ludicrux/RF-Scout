import mysql.connector as db
import json
import sys
from time import sleep
import re
import argparse
import threading
import glob
import logging
import logging.handlers



class DeltaLog(threading.Thread):
    def __init__(self, macaddress, RFC):
        threading.Thread.__init__(self)
        self.RFC = RFC
        self.mac_arg = macaddress
        
        self.scid = ("Delta%s" % self.mac_arg)
        
        self.newbaseval = False
        self.scan_selected = False
        self.mostrecent_id = 0
        self.printcounter = 0
        self.mostrecentTog = False
        self.counter = 0
        self.deltalog_scan = 0
        self.initset = False
        
        
        self.sqlpull = self.RFC.sqlpull
        self.sqlpush = self.RFC.sqlpush
        self.mostrecent = self.RFC.mostrecent
        self.script = self.RFC.script
        
        self.name_id = ("Delta/Log %s;" % macaddress)
        
        self.mostrecent.mostRecentSetup(self.scid)
        
        #print("%s Initializing" % self.name_id)
        logging.basicConfig(filename='logs/%s.log' % self.mac_arg, level=logging.DEBUG,format='%(asctime)s %(message)s')
        
        query = ("insert into delta (scan_data, base_time, macaddress) values ('[0.0]', 0, %s)" % (self.mac_arg))
        self.script.tableSetup(self.mac_arg, "delta", query)


    def baseline(self, scan):
        self.newbaseval = False
        self.deltalog_scan = scan
        self.sortlist = []
        self.basedictionary = {}
        self.entry = []
        
        if (self.deltalog_scan == 0) or (self.deltalog_scan == ""):
            self.scan_selected = False
            self.mostrecent.mostRecentScriptStatus(self.scid, False)
            return
        
        #request scan
        req = "select scan_data, time_stamp from scan where entry_id = %s AND macaddress = %s" % (self.deltalog_scan, self.mac_arg)
        self.sqlpull.pull_req(self.scid, req, "small")
        
        waiting = True
        #wait till the data is in
        while waiting:
            if self.sqlpull.currentstatus(self.scid):
                try:
                    items = self.sqlpull.pull_result(self.scid)
                except:
                    items = ""
                if items == "" or items == "[]":
                    logging.warning("[Delta] Selected scan not available")
                    fail = True
                    waiting = False
                else:
                    items = items[0]
                    fail = False
                    waiting = False
                    
        if fail == True:
            self.scan_selected = False
        elif fail == False:
            entry = []
            data, self.time = items
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
                self.basedictionary[freq] = dBm
                self.sortlist.append(freq)
            self.scan_selected = True

    def run(self):
        sleep(2)
        while self.RFC.deltaSwitch:
            if self.RFC.deltaSwitch == False:
                return
            sleep(0.5)
            #script config request
            self.scripthold = self.script.scriptConfigGet(self.mac_arg)
            
            if not self.scripthold:
                #print("%s no config found" % self.name_id)
                sleep(2)
                stuck = True
                while stuck:
                    self.scripthold = self.script.scriptConfigGet(self.mac_arg)
                    if self.scripthold != False:
                        stuck = False
                        self.initset = True
                        self.scriptstatus = self.scripthold[6]
                        self.newscan = self.scripthold[7]

                        if self.newscan != self.deltalog_scan:
                            #print("%s New scan selected" % self.name_id)
                            self.deltalog_scan = self.newscan
                            self.baseline(self.deltalog_scan)

            else:
                #if its not empty
                self.initset = True
                self.scriptstatus = self.scripthold[6]
                self.newscan = self.scripthold[7]
                if self.newscan != self.deltalog_scan:
                    #print("%s New scan selected" % self.name_id)
                    self.deltalog_scan = self.newscan
                    self.baseline(self.deltalog_scan)

            if self.RFC.killall:
                #print("%s stopping" % self.name_id)
                return

            if self.scriptstatus == 0:
                self.mostrecent.mostRecentScriptStatus(self.scid, False)
                sleep(5)
            
            if self.scan_selected and self.scriptstatus == 1:
                self.mostrecent.mostRecentScriptStatus(self.scid, True)
                if self.mostrecent.mostRecentStatus(self.mac_arg, self.scid):
                    self.newval = self.mostrecent.mostRecentGet(self.mac_arg, self.scid)
                    self.currententry_id, data, time = self.newval
                    self.mostrecentdata = data
                    self.mostrecentTog = True

            if self.scan_selected and self.mostrecentTog:
                self.mostrecentTog = False
                deltadictionary = {}
                entry = []
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
                    if freq in self.basedictionary:
                        dbmcompare = self.basedictionary.get(freq,"none")
                        calc = dBm-dbmcompare
                        deltadictionary[freq] = calc
                    else:
                        deltadictionary[freq] = 0
            
                final_list = []
                for index in self.sortlist:
                    if index in deltadictionary:
                        holddBm = deltadictionary.get(index,"none")
                    else:
                        holddBm = int(0)
                    final_entry = [index, holddBm]
                    final_list.append(final_entry)
            
                ###send new entry to update
                request = "Update delta set scan_data = '%s', base_time = '%s' where macaddress = %s" % (final_list, self.time, self.mac_arg)
                self.mostrecent.mostRecentCompleted(self.currententry_id, self.scid, request)
                #updateitem = [("Update delta set scan_data = '%s', base_time = '%s' where macaddress = %s" % (final_list, self.time, self.mac_arg)),"update"]
                #self.sqlpush.push_req(updateitem, "small")

                #print("%s New result queued" % self.name_id)
                

            elif self.scan_selected == False:
                sleep(1)
                self.counter += 1
                if self.counter == 10:
                    #print("%s no scan selected" % self.name_id)
                    self.counter = 0













