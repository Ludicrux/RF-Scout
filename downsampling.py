import mysql.connector as db
import json
import sys
from time import sleep
import re
import argparse
import threading
import datetime



class Downsampling(threading.Thread):
    def __init__(self, macaddress, RFC):
        threading.Thread.__init__(self)
        self.RFC = RFC
        self.mac_arg = macaddress
        
        self.scid = ("Downsampler%s" % self.mac_arg)
        self.name_id = ("Downsampler %s;" % macaddress)
        
        print("%s Initializing" % self.name_id)
    
        self.sqlpull = self.RFC.sqlpull
        self.sqlpush = self.RFC.sqlpush
        self.mostrecent = self.RFC.mostrecent
        self.script = self.RFC.script
        self.deleting = []
        self.sleep_time = 10
    
    
    def run(self):
        while self.RFC.downsampSwitch:
            self.scripthold = self.script.scriptConfigGet(self.mac_arg)
            
            if not self.scripthold:
                print("%s no config found" % self.name_id)
                sleep(2)
                stuck = True
                while stuck:
                    self.scripthold = self.script.scriptConfigGet(self.mac_arg)
                    if self.scripthold != False:
                        stuck = False
                        self.initset = True
                        self.scriptstatus = self.scripthold[9]
                        self.scriptlevel = self.scripthold[10]

            else:
                self.initset = True
                self.scriptstatus = self.scripthold[9]
                self.scriptlevel = self.scripthold[10]

            if self.initset == True and self.scriptstatus == 1:
                self.initset = False
                req = self.selection(-1, -2, self.scriptlevel*5)
                if req != False:
                    self.sqlpush.push_req([req, "delete"], "large")
                sleep(self.sleep_time)
                req = self.selection(-2, -4, self.scriptlevel*2)
                if req != False:
                    self.sqlpush.push_req([req, "delete"], "large")
                sleep(self.sleep_time)
                req = self.selection(-4, -10, self.scriptlevel*3)
                if req != False:
                    self.sqlpush.push_req([req, "delete"], "large")
                sleep(self.sleep_time)
                req = self.selection(-10, -30, self.scriptlevel*2)
                if req != False:
                    self.sqlpush.push_req([req, "delete"], "large")
                sleep(self.sleep_time)
                req = self.selection(-30, -100, self.scriptlevel*2)
                if req != False:
                    self.sqlpush.push_req([req, "delete"], "large")
                sleep(self.sleep_time)
            elif self.scriptstatus == 0:
                sleep(2)
                
                

    
    def selection(self, from_num, to_num, thresh):
        currentDT = datetime.datetime.now()
        
        close_date = currentDT + datetime.timedelta(days=from_num)
        close_date = close_date.strftime("%Y-%m-%d %H:%M:%S")
        further_date = currentDT + datetime.timedelta(days=to_num)
        further_date = further_date.strftime("%Y-%m-%d %H:%M:%S")
        
        req = ("Select entry_id from scan where macaddress = %s and time_stamp >= '%s' and time_stamp < '%s'" % (self.mac_arg, further_date, close_date))
        self.sqlpull.pull_req(self.scid, req, "medium")
        timeout = 0
        waiting = True
        while waiting:
            sleep(0.5)
            timeout += 1
            if timeout == 100:
                return False
            if self.sqlpull.currentstatus(self.scid):
                items = self.sqlpull.pull_result(self.scid)
                waiting = False
    
    
        id_list = []
        deleting = []
        for rows in items:
            id_list.append(rows[0])
        if len(id_list) != 0 and len(id_list) > thresh:
            distr = (len(id_list) / thresh)
            if distr != 0:
                for count, element in enumerate(id_list, 1):
                    if count % distr == 0:
                        deleting.append("delete From scan where entry_id = %s" % element)
                return deleting
        else: return False


