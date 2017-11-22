import rpyc
import uuid
import os

from rpyc.utils.server import ThreadedServer

DATA_DIR = "/tmp/minion/"


# TODO remove all block_uuid, address files by full path
class DataService(rpyc.Service):
    class exposed_DataServer():
        blocks = {}

        def exposed_put(self, file_path, data, minions):
            with open(DATA_DIR + str(file_path), 'wb') as f:
                f.write(data)
            if len(minions) > 0:
                self.forward(file_path, data, minions)

        def exposed_get(self, file_path):
            file = DATA_DIR + str(file_path)
            if not os.path.isfile(file):
                return None
            with open(file) as f:
                return f.read()

        # TODO: improve, no ideas
        def forward(self, file_path, data, minions):
            print("8888: forwaring to:")
            print(file_path, minions)
            minion = minions[0]
            minions = minions[1:]
            host, port = minion

            con = rpyc.connect(host, port=port)
            minion = con.root.Minion()
            minion.put(file_path, data, minions)

        # TODO: Handle exceptions
        def delete_file(self, file_path):
            os.remove(file_path)


if __name__ == "__main__":
    if not os.path.isdir(DATA_DIR): os.mkdir(DATA_DIR)
    t = ThreadedServer(DataService, port=8888)
    t.start()
