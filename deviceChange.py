import mysql.connector as db
import json
import sys
from time import sleep
import re
import argparse
import threading
import logging
from config import config as cfg

logging.basicConfig(filename='logs/main.log',level=logging.DEBUG,format='%(asctime)s %(message)s')


class device(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.newDevice = False
        while True:
            try:
                self.connection = db.MySQLConnection(host=cfg.mysql['adress'], user=cfg.mysql['login'], passwd=cfg.mysql['passw'], db=cfg.mysql['datab'])
                self.cursor = self.connection.cursor(buffered= True)
                break
            except:
                logging.warning("[DeviceChanges] Can't connect to the database, retrying....")
        self.mostRecent = 0
        self.newScan = False
        self.device_switch = False

    def base_devices(self):
        self.cursor.execute("select macaddress from ScriptConfig")
        self.connection.commit()
        self.device_list = []
        self.newDevices = []
        for devices in self.cursor:
            self.device_list.append(devices[0])
            self.newDevices.append(devices[0])

    def run(self):
        while self.device_switch:
            self.cursor.execute("select macaddress from ScriptConfig")
            self.connection.commit()
            for devices in self.cursor:
                if devices[0] not in self.device_list:
                    self.device_list.append(devices[0])
                    self.newDevices.append(devices[0])
                    self.newDevice = True

    def return_devices(self):
        devices = self.newDevice
        self.newDevices = []
        return devices

    def dataUpdate(self, macaddress):
        self.cursor.execute("select average_state, average_span, average_accuracy, peakhold_state, peakhold_toggle, deltalog_state, deltalog_scan, difflog_state, downsamp_state, downsamp_level from ScriptConfig where macaddress = %s" % macaddress)
        self.connection.commit()
        for items in self.cursor:
            self.average_state, self.average_span, self.average_accuracy, self.peakhold_state, self.peakhold_toggle, self.deltalog_state, self.deltalog_scan, self.difflog_state, self.downsamp_state, self.downsamp_level = items














