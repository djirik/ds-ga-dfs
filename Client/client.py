import rpyc
from clint.textui import colored
import sys
import os


def send_to_ds(file_path, data, data_servers, mdate):
    print("sending: " + str(data_servers))
    data_server = data_servers[0]
    data_servers = data_servers[1:]
    host, port = data_server
    con = rpyc.connect(host, port=port)
    data_server = con.root.DataServer()
    data_server.put(file_path, mdate, data, data_servers)


def read_from_ds(file_path, data_server):
    host, port = data_server
    con = rpyc.connect(host, port=port)
    data_server = con.root.DataServer()
    return data_server.get(file_path)

def File_Size_From_DS(file_path, data_server):
    host, port = data_server
    con = rpyc.connect(host, port=port)
    data_server = con.root.DataServer()
    return data_server.File_Size(file_path)

# TODO: write data to disk -> done
# def get(name_server, filename): add writing to file where dest is file name
def get(name_server, filename, dest):
    if name_server.read(filename):
        a = read_from_ds(filename, name_server.get_data_servers()[0])
        #print(a)
        x = open(dest, "wb")
        x.write(a)


def put(name_server, source, filename):
    #if name_server.write(filename): error if file not exist
    if os.path.isfile(source): # checking if file is exist on client side before sending to name server
        if name_server.write(filename):
            f = open(source, 'rb')
            mdate = os.path.getmtime(source)
            data = f.read()
            # with open(source) as data:
            data_servers = name_server.get_data_servers()
            send_to_ds(filename, data, data_servers, mdate)
    else:
        print('Wrong or non-existing path')

def Size(name_server, filename):
    a = File_Size_From_DS(filename, name_server.get_data_servers()[0])
    print(a)

# TODO: Need to parse input string into array
def main():
    con = rpyc.connect("localhost", port=2131)
    #con = rpyc.connect("192.168.56.110", port=2131)
    print("""
**********************************
*************╔══╗*****************
*************╚╗╔╝*****************
*************╔╝(¯`v´¯)************
*************╚══`.¸.[DFS]*********
**********************************""")
    master = con.root.Master()
    cwd = ""
    str = ""
    while True:
        last = str
        str = input("user@" + cwd + "#: ")
        args = str.split(' ')

        # Current working dir
        if args[0] == "cwd":
            print("/" + cwd)

        # File operations
        try:
            if args[0] == "touch":
                master.touch(args[1], cwd)
            if args[0] == "ls":
                listls = master.read(cwd)
                for x in listls:
                    if 'file' in listls[x]:
                        #print(x + '  <--file')
                        print(colored.green(x))
                    else:
                        #print(x + '  <--dir')
                        print(colored.red(x))
            if args[0] == "rm":
                master.rm(args[1])
            if args[0] == "size":
                Size(master,args[1])
            # Print last operation
            if args[0] == "last":
                print(last)
            # Dir operations
            if args[0] == "mkdir":
                if args[1][0:1] == '/':
                    print('Forbidden character!')
                else:
                    master.mkdir(args[1], cwd)
            if args[0] == "rmdir":
                master.rmdir(args[1])
            if args[0] == "get":
                #get(master, args[1]) old without writing to file
                get(master, args[1], args[2])
            if args[0] == "put":
                if cwd == "":
                    put(master, source=args[1], filename=cwd + args[2])
                else:
                    put(master, source=args[1], filename=cwd + "/" + args[2])
            if args[0] == "cd":
                if master.cd(args[1]):
                    if args[1] == '/':
                        cwd = ""
                    else:
                        cwd = args[1]
        except IndexError:
            print('Wrong operation arguments')

if __name__ == "__main__":
    #main(sys.argv[1:])
    main()
