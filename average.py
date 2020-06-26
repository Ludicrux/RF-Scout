import mysql.connector as db
import json
import sys
from time import sleep
import re
import argparse
import threading
import datetime
import logging


class Average(threading.Thread):
    def __init__(self, macaddress, RFC):
        threading.Thread.__init__(self)
        self.RFC = RFC
        self.mac_arg = macaddress
        
        self.scidnorm = ("Average%s" % self.mac_arg)
        self.scid = ("Average1%s" % self.mac_arg)
        self.scid2 = ("Average2%s" % self.mac_arg)
        
        self.sqlpull = self.RFC.sqlpull
        self.sqlpush = self.RFC.sqlpush
        self.mostrecent = self.RFC.mostrecent
        self.script = self.RFC.script
        
        self.scanrec = False
        self.success = False
        self.selected = False
        self.lengthEntry = 0
        
        self.name_id = ("Average %s;" % macaddress)
        
        logging.basicConfig(filename='logs/%s.log' % self.mac_arg, level=logging.DEBUG,format='%(asctime)s %(message)s')

        logging.info("[Average] Starting...")
        
        #table setup
        logging.info("[Average] Setting up tables...")
        query = ("INSERT INTO average (macaddress, time_range, scan_data) VALUES (%s, 0, '[0,0]')" % self.mac_arg) 
        self.script.tableSetup(self.mac_arg, "average", query)
        logging.info("[Average] Tables set up")
        

    def run(self):
        while self.RFC.averageSwitch:
            if self.RFC.averageSwitch == False:
                exit()
            
            if self.RFC.killall:
                #print("%s stopping" % self.name_id)
                exit()
            
            self.scripthold = self.script.scriptConfigGet(self.mac_arg)
            if not self.scripthold:
                #print("%s no config found" % self.name_id)
                stuck = True
                while stuck:
                    self.scripthold = self.script.scriptConfigGet(self.mac_arg)
                    if self.scripthold != False:
                        stuck = False
                        self.initset = True
                        self.scriptstatus = self.scripthold[1]
                        self.scriptspan = self.scripthold[2]
                        self.scriptaccuracy = self.scripthold[3]
            else:
                self.initset = True
                self.scriptstatus = self.scripthold[1]
                self.scriptspan = self.scripthold[2]
                self.scriptaccuracy = self.scripthold[3]
    
            if self.scriptstatus == 0:
                    sleep(5)
        
            if self.initset and self.scriptstatus == 1:
                self.initset == False
                currentDT = datetime.datetime.now()
                close_date = currentDT + datetime.timedelta(hours=-1*self.scriptspan)
                req = ("SELECT entry_id FROM scan WHERE time_stamp >= '%s' AND macaddress = %s" % (close_date, self.mac_arg))
                #print("%s Made small request" % self.name_id)
                self.sqlpull.pull_req(self.scid, req, "small")

                waiting = True
                #wait till the data is in
                timeout = 0
                while waiting:
                    sleep(0.1)
                    timeout += 1
                    if timeout == 300:
                        self.lengthEntry = 0
                        break
                    if self.sqlpull.currentstatus(self.scid):
                        self.items = self.sqlpull.pull_result(self.scid)
                        self.lengthEntry = len(self.items)
                        waiting = False
            
                if self.lengthEntry > 2:
                    self.selection()
                
                if self.lengthEntry == 0:
                    sleep(5)
                #once the pull_req has come through, run the entry selections
                
                if self.selected:
                    self.selected = False
                    req2 = ("SELECT scan_data, time_stamp FROM scan WHERE (%s)" % (self.select_items))
                    self.sqlpull.pull_req(self.scid2, req2, "large")
                    logging.info("[Average] Requesting scan data over last %s hour(s)" % self.scriptspan)
                    
                    #wait till the data is in
                    waiting = True
                    timeout2 = 0
                    while waiting:
                        sleep(0.1)
                        timeout2 += 1
                        if timeout2 == 300:
                            break
                        if self.sqlpull.currentstatus(self.scid2):
                            self.data = self.sqlpull.pull_result(self.scid2)
                            self.scanrec = True
                            waiting = False
                            logging("[Average] Received average configuration with")
               
                if self.scanrec:
                    self.calc()
                    self.scanrec = False
                    
                if self.success:
                    pushreq = [("update average set time_range = '%s', scan_data = '%s' where macaddress = %s" % (self.scriptspan, self.finallist, self.mac_arg)), "update"]
                    self.sqlpush.push_req(pushreq, "small")
                    #print("%s New result queued" % self.name_id)
                    self.success = False
                
                

    def selection(self):
        countlist = []
        selectable = []
        for it in self.items:
            countlist.append(it[0])
        for count, element in enumerate(countlist, 1):
            if count % self.scriptaccuracy == 0:
                element = ("entry_id = %s OR " % element)
                selectable.append(element)
        self.select_items = ("".join(selectable))
        self.select_items = self.select_items[:-3]
        self.selected = True
    
    
    def calc(self):
        logging.info("[Average] Calculating average of last %s hours" % self.scriptspan)
        entry = []
        listtest = {}
        freqtest = {}
        start_freqs = []
        freqlist = []
        for values in self.data:
            toggle = True
            rows, time_stamp = values
            rows = str(rows[1:-1])
            length = rows.count('[')
            entry[0:length] = rows.split('[')
            del entry[0]
            for index in range(len(entry)):
                value = entry[index]
                entry[index] = value[:-3]
                freq, dBm = entry[index].split(',')
                freq = float(freq)
                if freq not in (freqlist):
                    freqlist.append(freq)
                dBm = float(dBm[1:])
                if freq in freqtest:
                    holdfreq =  freqtest.get(freq,"none")
                    freqtest[freq] = holdfreq+1
                else:
                    freqtest[freq] = 1
                if freq in listtest:
                    hold = listtest.get(freq,"none")
                    listtest[freq] =  dBm+hold
                else:
                    listtest[freq] = dBm

        self.finallist = []
        freqlist.sort()
        for index in freqlist:
            holdpars = listtest.get(index,"none")
            val = freqtest.get(index,"none")
            newval = holdpars/val
            newval = float("%.2f" % newval)
            key = float(index)
            final =  [key,newval]
            self.finallist.append(final)
        self.success = True



