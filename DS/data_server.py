import threading

import rpyc
import os
from os.path import getsize, join

from rpyc.utils.server import ThreadedServer


DATA_DIR = "files/"


def get_file_structure():
    try:
        conn = rpyc.connect('localhost', 2131)
        DataService.exposed_DataServer.file_dict = conn.root.read()
    except ConnectionError:
        print("NS not available")


def update():
    for root, dirs, files in os.walk(DATA_DIR[0:-1]):
        for name in files:
            str = join(root, name).replace('\\', '/')
            print(os.path.getmtime(str))


class DataService(rpyc.Service):
    class exposed_DataServer:
        file_dict = {}

        def exposed_put(self, file_path: str, mdate, data, data_servers) -> bool:
            #  Checks for date of file and writes file to server. Returns True on successful write. Also starts thread
            # for forwarding file to other DS
            to_write = False

            if not os.path.isfile(DATA_DIR + file_path):
                to_write = True
            elif mdate > os.path.getmtime(DATA_DIR + file_path):
                to_write = True
            if to_write:
                with open(DATA_DIR + str(file_path), 'wb') as f:
                    f.write(data)
                if len(data_servers) > 0:
                    forward = threading.Thread(self.forward, args=(file_path, mdate, data, data_servers))
                    forward.daemon = True
                    forward.start()
                    #self.forward(file_path, data, data_servers)
            return to_write

        def exposed_get(self, file_path):
            file = DATA_DIR + str(file_path)
            print(file)
            if not os.path.isfile(file):
                return None
            f = open(file, "rb")
            data = f.read()
            return data

        # TODO: improve, no ideas
        def forward(self, file_path, mdate, data, minions):
            print("8888: forwaring to:")
            print(file_path, minions)
            minion = minions[0]
            minions = minions[1:]
            host, port = minion

            con = rpyc.connect(host, port=port)
            minion = con.root.DataServer()
            minion.put(file_path, mdate, data, minions)

        # TODO: Handle exceptions
        def exposed_delete_file(self, file_path):
            if os.path.isfile(DATA_DIR + file_path):
                os.remove(file_path)

        def exposed_get_size(self, file_path: str):
            if os.path.isfile(DATA_DIR+file_path):
                return os.path.getsize(DATA_DIR + file_path)
            else:
                return None



if __name__ == "__main__":
    update()
    if not os.path.isdir(DATA_DIR):
        os.mkdir(DATA_DIR)
    t = ThreadedServer(DataService, port=9999)
    t.start()
