import rpyc
import configparser
import signal
import pickle
import sys
import os
from functools import reduce
import operator

from rpyc.utils.server import ThreadedServer


def int_handler(signal, frame):
    pickle.dump(MasterService.exposed_Master.file_table, open('fs.img', 'wb'))
    sys.exit(0)


def set_conf():
    conf = configparser.ConfigParser()
    conf.read_file(open('dfs.conf'))
    MasterService.exposed_Master.replication_factor = int(conf.get('master', 'replication_factor'))
    data_servers = conf.get('master', 'minions').split(',')
    for m in data_servers:
        id, host, port = m.split(":")
        MasterService.exposed_Master.data_servers[id] = (host, port)

    if os.path.isfile('fs.img'):
        MasterService.exposed_Master.file_table = pickle.load(open('fs.img', 'rb'))


class MasterService(rpyc.Service):

    class exposed_Master():
        file_table = {}  # serialized and back using pickle
        # block_mapping = {}
        data_servers = {}    # list of data servers

        # block_size = 0
        replication_factor = 0

        # Requires full file path
        def exposed_read(self, fname):
            map_list = fname.split('/')
            # TODO handle exception
            reduce(operator.getitem, map_list, self.__class__.file_table)
            return True

        # Requires full file path
        def exposed_write(self, fname):
            if self.exists(fname):
                pass  # ignoring for now, will delete it later

            self.__class__.file_table[fname] = []

            return True

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


        def exposed_mkdir(self, path, dir_name: str):
            map_list = path.split('/')
            dir_to_add = {dir_name: {}}
            reduce(operator.getitem, map_list, self.__class__.file_table).update(dir_to_add)


        def exposed_rmdir(self, path):
            map_list = path.split('/')
            reduce(operator.getitem, map_list[0:-1], self.__class__.file_table).pop('user')

        # TODO Need support for nested dictionaries: Done
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

        def exposed_get_data_servers(self):
            return self.__class__.data_servers

        # def get_num_blocks(self, size):
        #     return int(math.ceil(float(size) / self.__class__.block_size))

        def exists(self, file_path: str):
            map_list = file_path.split('/')
            if reduce(operator.getitem, map_list, self.__class__.file_table) == "file":
                return True
            else:
                return False


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