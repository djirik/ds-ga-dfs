import rpyc
import configparser
import signal
import pickle
import sys
import os
from functools import reduce
import operator

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


class MasterService(rpyc.Service):

    class exposed_Master():
        file_table = {}  # serialized and back using pickle
        # block_mapping = {}
        data_servers = []    # list of data servers

        # block_size = 0
        replication_factor = 0

        # Requires full file path, can
        # TODO handle exception
        def exposed_read(self, fname):
            map_list = fname.split('/')
            reduce(operator.getitem, map_list, self.__class__.file_table)
            return True

        # Requires full file path
        # TODO handle exception, check for dir
        def exposed_write(self, fname: str):
            # if self.exists(fname):
            #     pass  # ignoring for now, will delete it later
            map_list = fname.split('/')
            try:
                reduce(operator.getitem, map_list[0:-1], self.__class__.file_table).update({map_list[-1]: 'file'})
                print(self.__class__.file_table)
            except KeyError:
                return "Wrong path, try mkdir first."




        # Requires full file path
        # TODO handle exception
        def exposed_delete(self, fname):
            # ''' Add request to data server '''

            map_list = fname.split("/")
            success = False
            # Possibly handle lists in different way
            try:
                reduce(operator.delitem, map_list, self.__class__.file_table)
                success = True

            except:
                pass
            return success

        # Requires full file path
        # TODO handle exception
        def exposed_mkdir(self, dir_name: str, path = ""):
            map_list = path.split('/')
            dir_to_add = {dir_name: {}}

            if path == '':
                self.__class__.file_table.update(dir_to_add)
            else:
                reduce(operator.getitem, map_list, self.__class__.file_table).update(dir_to_add)
            print(self.__class__.file_table)

        # Requires full file path
        # TODO handle exception
        def exposed_rmdir(self, path):
            map_list = path.split('/')
            reduce(operator.getitem, map_list[0:-1], self.__class__.file_table).pop('user')

        # TODO Need support for nested dictionaries: Done
        # we don't really need this.
        def exposed_get_file_table_entry(self, path: str):
            # File has to be stored as dict with single value equal to "file"
            map_list = path.split("/")
            try:
                tmp = reduce(operator.getitem, map_list, self.__class__.file_table)
            except:
                tmp = None

            return tmp
            # if fname in self.__class__.file_table:
            #     return self.__class__.file_table[fname]
            # else:
            #     return None

        def exposed_cd(self, path: str):
            self.exposed_read(path)

        def exposed_get_data_servers(self):
            return self.__class__.data_servers

        # Requires full file path
        # TODO handle exception
        def exists(self, file_path: str):
            map_list = file_path.split('/')
            if reduce(operator.getitem, map_list, self.__class__.file_table) == "file":
                return True
            else:
                return False

        # No chunks functionality
        # def alloc_blocks(self, dest, num):
        #     blocks = []
        #     for i in range(0, num):
        #         block_uuid = uuid.uuid1()
        #         nodes_ids = random.sample(self.__class__.data_servers.keys(), self.__class__.replication_factor)
        #         blocks.append((block_uuid, nodes_ids))
        #
        #         self.__class__.file_table[dest].append((block_uuid, nodes_ids))
        #
        #     return blocks


if __name__ == "__main__":
    set_conf()
    signal.signal(signal.SIGINT, int_handler)
    t = ThreadedServer(MasterService, port=2131)
    t.start()
