import mysql.connector as db
import json
import sys
from time import sleep
import re
import argparse
import threading


adress = "95.97.78.84"
login = "rfscoutuser"
passw = "ahphae8eVeethu"
datab = "rfscout"

#peak202481592263953

class Peakhold(threading.Thread):
    def __init__(self, macaddress, RFC):
        
        threading.Thread.__init__(self)
        self.RFC = RFC
        self.mac_arg = macaddress
        
        self.sqlpull = self.RFC.sqlpull
        self.sqlpush = self.RFC.sqlpush
        self.mostrecent = self.RFC.mostrecent
        self.script = self.RFC.script
        
        
        self.scid = ("Peak%s" % self.mac_arg)
        self.mostrecent.mostRecentSetup(self.scid)
        

        self.peakhold_state = 0
        self.mostrecent_id = 0
        self.mostrecentTog = False
        self.baseline_set = False
        
        self.name_id = ("Peakhold %s;" % macaddress)
        
        #print("%s initializing" % self.name_id)
    
        query = ("insert into peakhold (scan_data, base_id, macaddress) values ('[0.0]', 0, %s)" % (self.mac_arg))
        self.script.tableSetup(self.mac_arg, "peakhold", query)
        
    

    
    def baseline(self):
        if self.mostrecent.mostRecentStatus(self.mac_arg, self.scid):
            self.newval = self.mostrecent.mostRecentGet(self.mac_arg, self.scid)
        else:
            return

        self.sortlist = []
        self.peakdictionary = {}
        entry = []
        self.base_id, data, time = self.newval
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
            self.peakdictionary[freq] = dBm
            self.sortlist.append(freq)
        self.baseline_set = True


    def run(self):
        while self.RFC.peakSwitch:
            if self.RFC.peakSwitch == False:
                return
            sleep(1)
            
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
                        self.scriptstatus = self.scripthold[4]
                        self.scripttoggle = self.scripthold[5]
        
            else:
                self.initset = True
                self.scriptstatus = self.scripthold[4]
                self.scripttoggle = self.scripthold[5]


            if not self.baseline_set:
                self.mostrecent.mostRecentScriptStatus(self.scid, False)
                self.baseline()
            
            if self.mostrecent.mostRecentStatus(self.mac_arg, self.scid):
                self.newval = self.mostrecent.mostRecentGet(self.mac_arg, self.scid)
                self.mostrecentTog = True
            
            if self.RFC.killall:
                #print("%s stopping" % self.name_id)
                return


            if self.scripttoggle == 1 and self.mostrecentTog and self.baseline_set:
                self.mostrecent.mostRecentScriptStatus(self.scid, True)
                #print("%s Calculating" % self.name_id)
                self.mostrecentTog = False
                entry = []
                self.currententry_id = self.newval[0]
                data = self.newval[1]
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
                    if freq in self.peakdictionary:
                        dbmcompare = self.peakdictionary.get(freq,"none")
                        if dBm > dbmcompare:
                            self.peakdictionary[freq] = dBm
                    else:
                        self.peakdictionary[freq] = dBm
                    if freq not in self.sortlist:
                        self.sortlist.append(freq)
                        
                
                self.sortlist.sort()
                final_list = []
                for sorting in self.sortlist:
                    if sorting in self.peakdictionary:
                        holddBm = self.peakdictionary.get(sorting,"none")
                    else:
                        holddBm = int(0)
                    final_entry = [sorting, holddBm]
                    final_list.append(final_entry)
                
                request = ("Update peakhold set scan_data = '%s', base_id = %s where macaddress = %s" % (final_list, self.base_id, self.mac_arg))
                self.mostrecent.mostRecentCompleted(self.currententry_id, self.scid, request)
                
                #updateitem = [("Update peakhold set scan_data = '%s', base_id = %s where macaddress = %s" % (final_list, self.base_id, self.mac_arg)),"update"]
                #self.sqlpush.push_req(updateitem, "small")

                #print("%s New result queued" % self.name_id)
                
            elif self.scripttoggle == 2:
                self.mostrecent.mostRecentScriptStatus(self.scid, False)
                sleep(5)
            
            elif self.scripttoggle == 3:
                modechange = ("update ScriptConfig set peakhold_toggle = 2 where macaddress = %s" % (self.mac_arg))
                self.sqlpull.push_req(modechange, "small")
                self.baseline()




















