import operator
import threading
from functools import reduce

import errno
import rpyc
import os
from os.path import getsize, join

import sys

import time
from rpyc.utils.server import ThreadedServer

DATA_DIR = "files/"


def get_file_structure():
    try:
        conn = rpyc.connect('localhost', 2131)
        ns = conn.root.Master()
        DataService.exposed_DataServer.file_dict = ns.read()
    except ConnectionError:
        print("NS not available")


def update():
    get_file_structure()
    for root, dirs, files in os.walk(DATA_DIR):
        for name in files:
            file_path = join(root, name)
            map_list = file_path.split('/')
            structure = DataService.exposed_DataServer.file_dict
            print(structure)
            tmp = reduce(operator.getitem, map_list[1:], structure)
            tmp = 'lol'
            print(tmp)


class DataService(rpyc.Service):
    class exposed_DataServer:
        file_dict = {}

        # def __init__(self):
        #     self.update()

        def get_file_structure(self):
            try:
                conn = rpyc.connect('localhost', 2131)
                ns = conn.root.Master()
                DataService.exposed_DataServer.file_dict = ns.read()
            except ConnectionError:
                print("NS not available")

        def update(self):
            get_file_structure()
            for root, dirs, files in os.walk(DATA_DIR):
                for name in files:
                    file_path = join(root, name)
                    map_list = file_path.split('/')
                    structure = self.file_dict
                    print(structure)
                    tmp = reduce(operator.getitem, map_list[1:], structure)
                    tmp = 'lol'
                    print(tmp)

        def exposed_put(self, file_path: str, mdate, data, data_servers) -> bool:
            #  Checks for date of file and writes file to server. Returns True on successful write. Also starts thread
            # for forwarding file to other DS
            to_write = False

            if not os.path.isfile(DATA_DIR + file_path):
                to_write = True
            elif mdate > os.path.getmtime(DATA_DIR + file_path):
                to_write = True
            if to_write:
                if not os.path.exists(os.path.dirname(DATA_DIR + file_path)):
                    try:
                        os.makedirs(os.path.dirname(DATA_DIR + file_path))
                    except OSError as exc:  # Guard against race condition
                        if exc.errno != errno.EEXIST:
                            raise
                with open(DATA_DIR + str(file_path), 'wb') as f:
                    f.write(data)
                if len(data_servers) > 0:
                    print('Start forwarding to ' + str(data_servers))
                    # forward = threading.Thread(target=self.forward, args=(file_path, mdate, data, data_servers))
                    # forward.daemon = True
                    # forward.start()
                    # time.sleep(5)
                    self.forward(file_path, mdate, data, data_servers)
            return to_write

        def exposed_get(self, file_path):
            file = DATA_DIR + str(file_path)
            print(file)
            if not os.path.isfile(file):
                return None
            f = open(file, "rb")
            data = f.read()
            return data

        def exposed_file_size(self, file_path):
            file = DATA_DIR + str(file_path)
            print(file)
            if not os.path.isfile(file):
                return None
            Size = os.path.getsize(file)
            for Unit in ['bytes', 'KB', 'MB', 'GB', 'TB']:
                if Size < 1024.0:
                    return "%3.1f %s" % (Size, Unit)
                Size /= 1024.0

        # TODO: improve, no ideas
        # @staticmethod
        def forward(self, file_path, mdate, data, data_servers):
            print("forwaring to:")
            print(file_path, data_servers)
            ds = data_servers[0]
            data_servers = data_servers[1:]
            host, port = ds

            con = rpyc.connect(host, port=port)
            ds = con.root.DataServer()
            ds.put(file_path, mdate, data, data_servers)

        # TODO: Handle exceptions
        def exposed_delete_file(self, file_path):
            if os.path.isfile(DATA_DIR + file_path):
                os.remove(file_path)

        def get_file_dict(self) -> dict:
            return self.file_dict

if __name__ == "__main__":
    # get_file_structure()
    # update()
    # tmp = sys.argv()
    if not os.path.isdir(DATA_DIR):
        os.mkdir(DATA_DIR)
    t = ThreadedServer(DataService, port=int(sys.argv[1]))
    t.start()
