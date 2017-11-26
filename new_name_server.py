from typing import Union

import threading
import rpyc
import configparser
import signal
import pickle
import sys
import os
from functools import reduce
import operator

import time
from rpyc.utils.server import ThreadedServer


def int_handler(sig, frame):
    pickle.dump(MasterService.exposed_Master.file_table, open('fs.img', 'wb'))
    sys.exit(0)


def set_conf():
    conf = configparser.ConfigParser()
    conf.read_file(open('dfs.conf'))
    MasterService.exposed_Master.replication_factor = int(conf.get('master', 'replication_factor'))
    data_servers = conf.get('master', 'data_servers').split(',')
    for m in data_servers:
        id, host, port = m.split(":")
        MasterService.exposed_Master.data_servers.append((host, port))

    if os.path.isfile('fs.img'):
        MasterService.exposed_Master.file_table = pickle.load(open('fs.img', 'rb'))
        print(MasterService.exposed_Master.file_table)


def data_polling(data_servers: list):
    while True:
        MasterService.exposed_Master.available_data_servers = []
        for server in data_servers:
            try:
                conn = rpyc.connect(server[0], server[1])
                try:
                    conn.ping(timeout=5)
                    MasterService.exposed_Master.available_data_servers.append(server)
                except:
                    pass

            except ConnectionError:
                print(str(server) + " is not responding.")
        print("Available DS: " + str(MasterService.exposed_Master.available_data_servers))
        time.sleep(5)


class MasterService(rpyc.Service):

    class exposed_Master:
        """
        So far we have following public methods that can be called from anywhere: (note that exposed is omitted when you
        are calling these methods, e.g. you should call read() instead of exposed_read())

        exposed_read()              returns dict or None
        exposed_cd()                returns bool

        exposed_write()             returns bool

        exposed_rm()                returns bool
        exposed_rmdir()             returns bool
        exposed_mkdir()             returns bool

        exposed_get_data_servers()  returns list

        """

        file_table = {}  # serialized and back using pickle
        data_servers = []    # list of all data servers
        available_data_servers = []  # list of available data servers
        replication_factor = 0

        # Requires full file path, can
        # TODO handle exception
        def exposed_read(self, path: str) -> Union[dict, None]:
            """
            :param path: path to the file or dir
            :type path: str
            :return: On success: contents of path(dictionary) On fail: None
            """
            if path == '':
                return self.__class__.file_table
            else:
                map_list = path.split('/')
                path = map_list[0:-1]
                if self.exists(path):
                #if self.exists(map_list):  Error 
                    return reduce(operator.getitem, map_list, self.__class__.file_table)
                else:
                    return None

        # Requires full file path
        # TODO handle exception
        def exposed_write(self, full_path="") -> bool:
            """
            :param full_path: Path where to write to. including filename
            :type full_path: str
            :return: True on success, False on fail
            :rtype: bool
            """
            # If fname is "" it will read contents of root directory
            map_list = full_path.split('/')
            file_name = map_list[-1]
            path = map_list[0:-1]

            if self.exists(path):
                reduce(operator.getitem, path, self.__class__.file_table).update({file_name: 'file'})
                print(self.__class__.file_table)
                return True
            else:
                return False

        # Requires full file path
        # TODO handle exception, add request to data servers
        def delete(self, full_path="") -> bool:
            #  Add request to data server : done
            """
            :param full_path:  Full path to the file
            :type full_path: str
            :returns bool:
            """
            map_list = full_path.split("/")
            obj = map_list[-1]
            if self.exists(map_list):

                tmp = reduce(operator.delitem, map_list[0:-1], self.__class__.file_table).pop(obj)
                # for each in self.__class__.data_servers:
                #     each.delete_file(obj)
                print(tmp)
                return True
            else:
                return False

        # Requires full file path
        # TODO handle exception
        def exposed_mkdir(self, dir_name: str, path=""):
            map_list = path.split('/')
            dir_to_add = {dir_name: {}}

            if path == '':
                self.__class__.file_table.update(dir_to_add)
            else:
                reduce(operator.getitem, map_list, self.__class__.file_table).update(dir_to_add)
            print(self.__class__.file_table)

        def exposed_touch(self, file_name: str, path=""):
            map_list = path.split('/')
            file_to_add = {file_name: 'file'}

            if path == '':
                self.__class__.file_table.update(file_to_add)
            else:
                reduce(operator.getitem, map_list, self.__class__.file_table).update(file_to_add)
            print(self.__class__.file_table)

        # Requires full file path
        # TODO handle exception
        def exposed_rmdir(self, path="") -> bool:
            """
            :type path: str
            :param path: Full path to object to be deleted
            :return:
            :rtype: bool
            """
            return self.delete(path)

        def exposed_cd(self, path: str) -> bool:
            """
            :param path: Path to cd to
            :return: Dict if dir exists
            :rtype: dict
            :rtype: bool
            """
            return self.exists(path.split('/'))

        def exposed_get_data_servers(self):
            return self.available_data_servers
            #return self.data_servers

        # Requires full file path
        def exists(self, path: list = []) -> bool:
            """
            :param path: Full path to dir or file
            :type path: list
            :return: True of False
            :rtype: bool
            """
            if path == ['']:
                return True
            else:
                try:
                    tmp = reduce(operator.getitem, path, self.__class__.file_table)
                    if type(tmp) is dict:
                        return True
                    else:
                        return False
                except KeyError:
                    return False



if __name__ == "__main__":
    set_conf()
    polling = threading.Thread(target=data_polling, args=(MasterService.exposed_Master.data_servers,), name='Polling')
    polling.daemon = True
    polling.start()
    signal.signal(signal.SIGINT, int_handler)
    signal.signal(signal.SIGTERM, int_handler)
    t = ThreadedServer(MasterService, port=2131)
    t.start()

