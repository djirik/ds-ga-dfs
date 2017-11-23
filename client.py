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
    str = ""
    while True:
        last = str
        str = input("user@" + cwd + "#: ")
        args = str.split(' ')

        # Current working dirqweq
        if args[0] == "cwd":
            print("/" + cwd)

        # File operations
        try:
            if args == "touch":
                pass
            if args[0] == "ls":
                print(master.read(cwd))
            if args[0] == "rm":
                pass
            if args[0] == "size":
                pass

            # Print last operation
            if args[0] == "last":
                print(last)

            # Dir operations
            if args[0] == "mkdir":
                master.mkdir(args[1], cwd)
            if args[0] == "rmdir":
                master.rmdir(args[1])
            if args[0] == "get":
                get(master, args[1])
            if args[0] == "put":
                if cwd == "":
                    put(master, source=args[1], filename=cwd + args[2])
                else:
                    put(master, source=args[1], filename=cwd + "/" + args[2])
            if args[0] == "cd":
                if master.cd(args[1]):
                    cwd = args[1]
        except IndexError:
            print('Wrong operation arguments')

if __name__ == "__main__":
    #main(sys.argv[1:])
    main()
