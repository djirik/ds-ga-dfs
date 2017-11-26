import operator
from functools import reduce
import errno
import rpyc
import os
from os.path import join
import sys
import time
from rpyc.utils.server import ThreadedServer

DATA_DIR = "files/"


def update():
    while True:
        try:
            conn = rpyc.connect('localhost', 2131)
            ns = conn.root.Master()
            DataService.exposed_DataServer.file_dict = ns.read()
            break
        except ConnectionError:
            print("NS not available")
        time.sleep(5)

    for root, dirs, files in os.walk(DATA_DIR):
        for name in files:
            file_path = join(root, name)
            print("File path: " + str(file_path))
            map_list = file_path.split('/')
            test_map_list = ['qwe']
            structure = DataService.exposed_DataServer.file_dict
            print(DataService.exposed_DataServer.file_dict)
            try:
                tmp = reduce(operator.getitem, test_map_list, structure)
                if tmp == 'file':
                    pass
                #tmp = 'lol'
                #print(tmp)
            except KeyError:
                pass


class DataService(rpyc.Service):
    class exposed_DataServer:
        file_dict = {}

        def exposed_put(self, file_path: str, mdate, data, data_servers) -> bool:
            #  Checks for date of file and writes file to server. Returns True on successful write.
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
                    # TODO: if some can make it work it would be great, it works good enough for small files.
                    # forward = threading.Thread(target=self.forward, name='Forwarder',
                    #                            args=(file_path, mdate, data, data_servers_not_referenced))
                    # forward.daemon = True
                    # forward.start()
                    # forward.join()
                    # print('forwader started')
                    #
                    # time.sleep(1)

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

        def exposed_Check_if_exist(self, file_path):
            file = DATA_DIR + str(file_path)
            print(file)
            return os.path.isfile(file)
        
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
        def forward(self, file_path, mdate, data, data_servers):

            print("forwaring to:")
            print(file_path, data_servers)


            dss = data_servers[:]
            #print(dss)
            ds = dss[0]
            servers = dss[1:]
            host, port = ds

            con = rpyc.connect(host, port=port)
            ds = con.root.DataServer()
            ds.put(file_path, mdate, data, servers)

        # TODO: Handle exceptions
        def exposed_delete_file(self, file_path):
            if os.path.isfile(DATA_DIR + file_path):
                os.remove(file_path)

        def get_file_dict(self) -> dict:
            return self.file_dict

if __name__ == "__main__":
    # update()
    if not os.path.isdir(DATA_DIR):
        os.mkdir(DATA_DIR)
    t = ThreadedServer(DataService, port=int(sys.argv[1]))
    t.start()
