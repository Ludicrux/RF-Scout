import deviceChange
import threading
import delta
import peakhold as peak
import average
import downsampling
from time import sleep


class RFC(threading.Thread):
    def __init__(self, macaddress, sqlpush, sqlpull, mostrecent, scriptqueue, menu):
        threading.Thread.__init__(self)
        self.mac = macaddress
        self.version = "version 2.0"
        
        self.enable_printing = True
        
        self.sqlpush = sqlpush
        self.sqlpull = sqlpull
        self.mostrecent = mostrecent
        self.script = scriptqueue
        self.menu = menu
        
        self.killall = False
        self.threadLive = True
        
        self.device = deviceChange.device()
        
        self.delta = delta.DeltaLog(self.mac, self)
        self.deltaSwitch = True
        self.delta.start()
    
        self.peak = peak.Peakhold(self.mac, self)
        self.peakSwitch = True
        self.peak.start()
        
        self.average = average.Average(self.mac, self)
        self.averageSwitch = True
        self.average.start()
        
        #self.downsamp = downsampling.Downsampling(self.mac, self)
        #self.downsampSwitch = True
        #self.downsamp.start()
        
        
    
    
    def run(self):
        while self.threadLive:
            if self.killall:
                self.deltaSwitch = False
                self.peakSwitch
                exit()
            sleep(1)

    def stop_all(self):
        self.killall = True
        self.threadLive = False
        self.deltaSwitch = False
        self.peakSwitch = False
        self.averageSwitch = False
        self.downsampSwitch = False




            





