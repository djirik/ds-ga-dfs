import rpyc
import sys
import os


def send_to_ds(file_path, data, data_servers):
    print("sending: " + str(data_servers))
    data_server = data_servers[0]
    data_servers = data_servers[1:]
    host, port = data_server

    con = rpyc.connect(host, port=port)
    data_server = con.root.DataServer()
    data_server.put(file_path, data, data_servers)


def read_from_ds(file_path, data_server):
    host, port = data_server
    con = rpyc.connect(host, port=port)
    data_server = con.root.DataServer()
    return data_server.get(file_path)


def get(name_server, filename):
    if name_server.read(filename):
        a = read_from_ds(filename, name_server.get_data_servers()[0])
        print(a)

        # with open(source) as data:
        #data_servers = name_server.get_data_servers()
        #send_to_ds(filename, data, data_servers)


def put(name_server, source, filename):
    if name_server.write(filename):
        f = open(source, 'rb')
        data = f.read()
        # with open(source) as data:
        data_servers = name_server.get_data_servers()
        send_to_ds(filename, data, data_servers)
    else:
        print('Wrong or non-existing path')

# TODO: Need to parse input string into array
def main():
    con = rpyc.connect("localhost", port=2131)
    master = con.root.Master()
    cwd = ""
    while True:

        str = input("user#:").split(' ')



        # Current working dirqweq
        if str == "cwd":
            pass

        # File operations
        if str == "touch":
            pass
        if str == "ls":
            pass
        if str[0] == "rm":
            pass
        if str == "size":
            pass

        # Last operation
        if str == "last":
            pass

        # Dir operations
        if str[0] == "mkdir":
            master.mkdir(str[1], cwd)
        if str == "rmdir":
            pass
        if str[0] == "get":
            get(master, str[1])
        if str[0] == "put":
            if cwd == "":
                put(master, source=str[1], filename=cwd + str[2])
            else:
                put(master, source=str[1], filename=cwd + "/" + str[2])


if __name__ == "__main__":
    #main(sys.argv[1:])
    main()
