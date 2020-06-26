from RFC import RFC
from time import sleep
import deviceChange
import threading
import Queue
import mysql.connector as db
import thread
import glob
import logging
import logging.handlers
import os
import re




deviceCheck = deviceChange.device()
deviceCheck.base_devices()
deviceCheck.start()
macaddress = []
deviceList = []



class sqlQueue(threading.Thread):
    def __init__(self, switch, type):
        self.mostrecent = {}
        self.mostrecentstat = {}
        self.mostrecentstat_id = {}
        self.scriptconfig = {}
        self.requestdict = {}
        self.status = {}
        self.counter = 0
        self.pushlock = True
        self.pulllock = True
        
        threading.Thread.__init__(self)
        #connection = db.MySQLConnection(host=self.adress, user=self.login, passwd=self.passw, db=self.datab)
        self.switch = switch
        self.type = type
        self.pullcounter = 0
        self.pullmax = 10
        
        self.pushcounter = 0
        self.pushmax = 10
        self.running = True
    
        self.mostrecentMode = {}
        self.mostrecentProc = {}
        self.entryIdCounter = {}
        self.mostrecentupload = {}
        self.maclist = []
        
        self.newDevices = []
        self.newDeviceToggle = False

        self.devicelistMAC = []

    def stopfn(self):
        print("stopping")
        self.switch = False
        exit()
    
    def startfn(self):
        self.switch = True
    
    def run(self):
        if self.type == "push":
            self.pushqueue = Queue.Queue()
            self.push()
        elif self.type == "pull":
            self.pullqueue = Queue.Queue()
            self.pull()
        elif self.type == "most recent":
            self.mostRecent()
        elif self.type == "script":
            self.scriptConfig()
        else:
            raise Exception("Invalid type")

    def push(self):
        while self.switch:
            sleep(0.5)
            if (self.pushqueue.qsize() > 0) and (self.pushcounter <= self.pushmax) and self.pushlock:
                self.pushlock = False
                thread.start_new_thread(self.push_thread,())
                sleep(0.5)

    def push_req(self, data, type):
        entry = [data, type]
        self.pushqueue.put(entry, False)
        return
    
    def push_thread(self):
        self.pushcounter += 1
        if (self.pushqueue.qsize() == 0):
            self.pushcounter -= 1
            return
        datatype, size = self.pushqueue.get_nowait()
        self.pushlock = True
        connection = db.MySQLConnection(host=cfg.mysql['adress'], user=cfg.mysql['login'], passwd=cfg.mysql['passw'], db=cfg.mysql['datab'])
        cursor = connection.cursor()
        data, type = datatype
        if size == "large":
            for i in data:
                cursor.execute(i)
            connection.commit()
        elif size == "small":
            cursor.execute(data)
            connection.commit()
        self.pushqueue.task_done()
        self.pushcounter -= 1
        cursor.close()
        connection.close()


    def pull(self):
        while self.switch:
            sleep(0.5)
            if (self.pullqueue.qsize() > 0) and (self.pullcounter <= self.pullmax) and (self.pulllock):
                self.pulllock = False
                thread.start_new_thread(self.pull_thread, ())
                

    def pull_req(self, id, request, type):
        if id not in self.status:
            self.status[id] = False
        if id not in self.requestdict:
            self.requestdict[id] = ""
        entry = [id, request]
        self.pullqueue.put(entry, False)
        
    def pull_thread(self):
        self.pullcounter += 1
        if (self.pullqueue.qsize() == 0):
            self.pullcounter -= 1
            return
        id, request =  self.pullqueue.get_nowait()
        self.pulllock = True
        connection = db.MySQLConnection(host=cfg.mysql['adress'], user=cfg.mysql['login'], passwd=cfg.mysql['passw'], db=cfg.mysql['datab'])
        cursor = connection.cursor()

        try:
            cursor.execute(request)
        except:
            self.pullqueue.task_done()
            cursor.close()
            connection.close()
            self.pullcounter -= 1
        returnval = []
        for item in cursor:
            returnval.append(item)
        
        self.requestdict[id] = returnval
        self.status[id] = True
        self.pullqueue.task_done()
        cursor.close()
        connection.close()
        self.pullcounter -= 1

    def pull_result(self, id):
        self.status[id] = False
        return self.requestdict.get(id, "none")

    def currentstatus(self, id):
        #pullstatus
        return self.status.get(id, False)
    
    def mostRecentSetup(self, scid):
        self.mostrecentstat_id[scid] = False
        self.mostrecentMode[scid] = False


    def mostRecent(self):
        while self.switch:
            connection = db.MySQLConnection(host=cfg.mysql['adress'], user=cfg.mysql['login'], passwd=cfg.mysql['passw'], db=cfg.mysql['datab'])
            cursor = connection.cursor()
            sleep(0.5)
            cursor.execute("select count(*) from scan")
            for item in cursor:
                count = item[0]
            if count > self.counter:
                self.counter = count
                cursor.execute("select macaddress, scan_data, time_stamp, entry_id from scan ORDER BY entry_id DESC limit 1")
                for itemd in cursor:
                    mac, data, time, entry_id = itemd
                    if mac not in self.maclist:
                        query = ("insert into buffer (entry_id, scan_data, macaddress) values (0, '[0.0]', %s)" % (mac))
                        self.tableSetup(mac, "buffer", query)
                        self.maclist.append(mac)
                        self.newDevices.append(mac)
                        self.newDeviceToggle = True
                    
                    listup = [entry_id, data, time]
                    self.mostrecent[mac] = listup
                    scid1 = ("Peak%s" % mac)
                    scid2 = ("Delta%s" % mac)
                    self.mostrecentstat_id[scid1] = True
                    self.mostrecentstat_id[scid2] = True
                    self.entryIdCounter[entry_id] = 0
                    thread.start_new_thread(self.mostRecentUpload, (entry_id, mac, data))
            cursor.close()
            connection.close()

    def mostRecentScriptStatus(self, scid, status):
        self.mostrecentMode[scid] = status


    def mostRecentUpload(self, entry_id, mac, data):
        scidPeak = ("Peak%s" % mac)
        scidDelta = ("Delta%s" % mac)
        scidentryPeak = ("Peak%s%s" % (mac, entry_id))
        scidentryDelta = ("Delta%s%s" % (mac, entry_id))
        val = 0
        timeout = 0
        scidPeakToggle = False
        scidDeltaToggle = False
        while True:
            sleep(0.5)
            if self.mostrecentMode.get(scidPeak, False):
                val += 1
                scidPeakToggle = True
            if self.mostrecentMode.get(scidDelta, False):
                val += 1
                scidDeltaToggle = True
            compareval = self.entryIdCounter.get(entry_id, 100)
            if compareval == val:
                connection = db.MySQLConnection(host=cfg.mysql['adress'], user=cfg.mysql['login'], passwd=cfg.mysql['passw'], db=cfg.mysql['datab'])
                cursor = connection.cursor()
                updateitem = ("Update buffer set entry_id = %s, scan_data = '%s' where macaddress = %s" % (entry_id, data, mac))
                cursor.execute(updateitem)
                if scidPeakToggle:
                    updateitemPeak = self.mostrecentupload.get(scidentryPeak, "")
                    cursor.execute(updateitemPeak)
                if scidDeltaToggle:
                    updateitemDelta = self.mostrecentupload.get(scidentryDelta, "")
                    cursor.execute(updateitemDelta)
                connection.commit()
                print("%s-%s; Uploaded bulk" % (entry_id, mac))
                cursor.close()
                connection.close()
                return
                
            else:
                val = 0
                timeout += 1

            if timeout == 300:
                print("timed out on %s" % entry_id)
                self.mostrecentMode[scidPeak] = False
                self.mostrecentMode[scidDelta] = False
                return


    def mostRecentCompleted(self, entry_id, scid, request):
        scidentry = ("%s%s" % (scid, entry_id))
        val = self.entryIdCounter.get(entry_id, 0)
        val += 1
        self.entryIdCounter[entry_id] = val
        self.mostrecentupload[scidentry] = request

    
    def mostRecentStatus(self, id, type_id):
        return self.mostrecentstat_id.get(type_id, False)

    def mostRecentGet(self, id, type_id):
        self.mostrecentstat_id[type_id] = False
        return self.mostrecent.get(id,"False")
    
    def newDevicesGet(self):
        templist = []
        for items in self.newDevices:
            templist.append(items)
        self.newDevices = []
        self.newDeviceToggle = False
        return templist

    def newDevicesStatus(self):
        if self.newDeviceToggle:
            self.newDeviceToggle = False
            return True
        if not self.newDeviceToggle:
            return False


    def tableSetup(self, mac, script, query):
        connection = db.MySQLConnection(host=cfg.mysql['adress'], user=cfg.mysql['login'], passwd=cfg.mysql['passw'], db=cfg.mysql['datab'])
        cursor = connection.cursor()
        cursor.execute("select count(*) from %s where macaddress = %s" % (script, mac))
        for updatecount in cursor:
            if updatecount[0] == 0:
                cursor.execute(query)
                connection.commit()
            elif updatecount[0] > 1:
                cursor.execute("delete from %s where macaddress = %s" % (script, mac))
                cursor.execute(query)
                connection.commit()
        cursor.close()
        connection.close()

    def scriptConfig(self):
        while self.switch:
            connection = db.MySQLConnection(host=cfg.mysql['adress'], user=cfg.mysql['login'], passwd=cfg.mysql['passw'], db=cfg.mysql['datab'])
            cursor = connection.cursor()
            cursor.execute("select macaddress, average_state, average_span, average_accuracy, peakhold_state, peakhold_toggle, deltalog_state, deltalog_scan, difflog_state, downsamp_state, downsamp_level from ScriptConfig")
            for items in cursor:
                mac = items[0]
                if mac not in self.devicelistMAC:
                    self.devicelistMAC.append(mac)
                self.scriptconfig[mac] = items
            cursor.close()
            connection.close()
            sleep(5)

    def scriptConfigGet(self, id):
        return self.scriptconfig.get(id, False)

    def stopAll(self):
        self.running = False


logging.basicConfig(filename='logs/main.log',level=logging.DEBUG,format='%(asctime)s %(message)s')
logging.info('Started main.py')
DeviceDict = {}

pushqueue = sqlQueue(True, "push")
pullqueue = sqlQueue(True, "pull")
mostrecentqueue = sqlQueue(True, "most recent")
scriptqueue = sqlQueue(True, "script")

pushqueue.start()
pullqueue.start()
mostrecentqueue.start()
scriptqueue.start()

print("Setting up systems")
sleep(2)
#setup device check and RFC
if mostrecentqueue.newDevicesStatus():
    jeff = mostrecentqueue.newDevicesGet()
    for items in jeff:
        print("new device found: %s" % items)
        items = RFC(items, pushqueue, pullqueue, mostrecentqueue, scriptqueue, logger)
        items.start()
        deviceList.append(items)


while True:
    try:
        if mostrecentqueue.newDevicesStatus():
            for items in mostrecentqueue.newDevicesGet():
                print("new device found: %s" % items)
                items = RFC(items, pushqueue, pullqueue, mostrecentqueue, scriptqueue, menu)
                items.start()
                deviceList.append(items)

    

    except (KeyboardInterrupt, SystemExit):
        pushqueue.stopAll()
        pullqueue.stopAll()
        mostrecentqueue.stopAll()
        scriptqueue.stopAll()
        for i in range(len(deviceList)):
            deviceList[i].stop_all()










