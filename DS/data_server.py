import operator
import threading
from functools import reduce
import errno
import rpyc
import os
from os.path import join
import sys
import time
from rpyc.utils.server import ThreadedServer

DATA_DIR = "files/"


def update(selfport):
    paths = []

    def get_and_write(port, file_path):
        dss = ns.get_data_servers()  # get list of data servers
        for each in dss:  #
            if each[1] != port:  # if the server is different from me

                ds_conn = rpyc.connect(each[0], each[1])  # initialize connection
                ds = ds_conn.root.DataServer()
                try:
                    data = ds\
                        .get(file_path.split('/', maxsplit=1)[1])  # try to get file from that server
                    ds_conn.close()
                    open(file_path, 'wb').write(data)  # and write

                    break  # if written - break
                except FileNotFoundError as err:
                    print(err)
                    print('File not found on: ' + str(each))  # other case - try other servers.

    def get_file(files: dict, current_path: str):
        for entry in files:
            if type(files[entry]) is dict:
                current_path += (str(entry)) + '/'
                # print(current_path)
                get_file(files[entry], current_path)
            else:
                paths.append(current_path + entry)
                #print(current_path + entry)

    while True:
        while True:
            try:
                conn = rpyc.connect('localhost', 2131)
                ns = conn.root.Master()
                DataService.exposed_DataServer.ns_file_dict = ns.read()
                break
            except ConnectionError:
                print("NS not available")
            time.sleep(5)

        ns_files = DataService.exposed_DataServer.ns_file_dict
        print(DataService.exposed_DataServer.ns_file_dict)

        # Find updates for existing files
        for root, dirs, files in os.walk(DATA_DIR):
            for name in files:
                file_path = join(root, name)
                # print("File path: " + str(file_path))
                map_list = file_path.replace('\\', '/').split('/')[1:]

                try:
                    tmp = reduce(operator.getitem, map_list, ns_files)  # try to retrive same file from NS records
                    if tmp[0] == 'file':                                # if found:
                        if tmp[1] > os.path.getmtime(file_path):        # check mod date
                            get_and_write(selfport, file_path)
                except KeyError:    # if file not found on NS, delete it here.
                    os.remove(file_path)

            # Find missing files here, use get_and_write(port)
        get_file(ns_files, '')

        for path in paths:
            if not os.path.isfile(str(DATA_DIR + path)):
                get_and_write(selfport, DATA_DIR + path)

        time.sleep(5)


class DataService(rpyc.Service):
    class exposed_DataServer:
        ns_file_dict = {}

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
                os.utime(DATA_DIR + str(file_path), (time.time(), mdate))
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
            ds = dss[0]
            servers = dss[1:]
            host, port = ds

            con = rpyc.connect(host, port=port)
            ds = con.root.DataServer()
            ds.put(file_path, mdate, data, servers)

        def exposed_forward_all(self, data_servers):
            for root, dirs, files in os.walk(DATA_DIR):
                for name in files:
                    file_path = join(root, name)
                    f = open(file_path)
                    data = f.read()
                    self.forward(file_path, os.path.getmtime(file_path), data, data_servers)


        # TODO: Handle exceptions
        def exposed_delete_file(self, file_path):
            if os.path.isfile(DATA_DIR + file_path):
                os.remove(file_path)

        def get_file_dict(self) -> dict:
            return self.ns_file_dict

if __name__ == "__main__":
    # update(int(sys.argv[1]))
    if not os.path.isdir(DATA_DIR):
        os.mkdir(DATA_DIR)
    updater = threading.Thread(name="Updater", target=update, args=(int(8888),), daemon=True)
    updater.start()

    t = ThreadedServer(DataService, port=int(sys.argv[1]))
    t.start()
