import rpyc
from clint.textui import colored
import sys
import os
import itertools
import time
import logging

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
    return data_server.file_size(file_path)

def File_Exist_DS(file_path, data_server): # testing if file is exist on DS
    host, port = data_server
    con = rpyc.connect(host, port=port)
    data_server = con.root.DataServer()
    return data_server.Check_if_exist(file_path)

def rm_DS(file_path, data_server): # testing if file is exist on DS
    host, port = data_server
    con = rpyc.connect(host, port=port)
    data_server = con.root.DataServer()
    return data_server.delete_file(file_path)

# TODO: write data to disk -> done
# def get(name_server, filename): add writing to file where dest is file name
def get(name_server, filename, dest):
    if File_Exist_DS(filename,name_server.get_data_servers()[0]):
        if name_server.read(filename):
            a = read_from_ds(filename, name_server.get_data_servers()[0])
            #print(a)
            x = open(dest, "wb")
            x.write(a)
    else:
        print("No such file on the DS")

def rm(name_server, filename):
    if File_Exist_DS(filename,name_server.get_data_servers()[0]):
        if rm_DS(filename,name_server.get_data_servers()[0]):
            return True
    else:
        print("No such file on the DS")
        return False

def put(name_server, source, filename):
    # if name_server.write(filename): error if file not exist
    if os.path.isfile(source):  # checking if file is exist on client side before sending to name server
        if name_server.can_write(filename):
            f = open(source, 'rb')
            mdate = os.path.getmtime(source)
            data = f.read()
            # with open(source) as data:
            data_servers = name_server.get_data_servers()
            send_to_ds(filename, data, data_servers, mdate)
            name_server.write(filename)
    else:
        print('Wrong or non-existing path')

def touch(name_server, filename, source_data):
    # if name_server.write(filename): error if file not exist
    if name_server.can_write(filename):
        mdate = time.time()
        data_full=''
        for data in source_data:
            data_full = data_full + data + ' '
        data = data_full.encode()
        # with open(source) as data:
        data_servers = name_server.get_data_servers()
        send_to_ds(filename, data, data_servers, mdate)
        name_server.write(filename)

def Size(name_server, filename):
    a = File_Size_From_DS(filename, name_server.get_data_servers()[0])
    print(a)

# TODO: Need to parse input user_inputing into array
def main():
    #con = rpyc.connect("192.168.56.110", port=2131)
    print("""
    **********************************
    *************╔══╗*****************
    *************╚╗╔╝*****************
    *************╔╝(¯`v´¯)************
    *************╚══`.¸.[DFS]*********
    **********************************""")
    cwd = ""
    user_input = ""
    prev_dirc = ""
    full_dir=''
    logging.basicConfig(format='%(asctime)s %(message)s', filename="client.log", level=logging.INFO)
    logging.info('Client is started')
    while True:
        try:
            con = rpyc.connect("localhost", port=2131)
            master = con.root.Master()
            while True:
                try:
                    last = user_input
                    user_input = input("user@" + full_dir + "#: ")
                    logging.info(user_input)
                    args = user_input.split(' ')

                    # Current working dir
                    if args[0] == "cwd":
                        print("/" + full_dir)

                    # File operations
                    try:
                        if args[0] == "touch":
                            if cwd == "":
                                touch(master,args[1], args[2:])
                            else:
                                touch(master,full_dir + '/' + args[1], args[2])
                        elif args[0] == "ls":
                            Files_List=['Files']
                            Dir_List=['Directories']
                            #listls = master.read(cwd) old, now ls ../ works fine
                            if args[1:] and args[1] == "../" and cwd != '': #check if 2nd arg is exist and its ../
                                listls = master.read(prev_dirc)
                            else:
                                listls = master.read(full_dir)
                            for x in listls:
                                if 'file' in listls[x]:
                                    #print(x + '  <--file')
                                    #print(colored.green(x))
                                    Files_List.append(x) # append to file list
                                else:
                                    #print(x + '  <--dir')
                                    #print(colored.red(x))
                                    Dir_List.append(x) # append to dir list
                            Max_Dir_Len= len(max(Dir_List, key=len))
                            Max_file_Len= len(max(Files_List, key=len))
                            print('|' + Max_Dir_Len*'=' + '|' + Max_file_Len*'=' + '|')
                            l=1 # header should be printed once
                            for col,col1 in itertools.zip_longest(Dir_List,Files_List,fillvalue=''):
                                print('|' +colored.red('{Dir_Name:{Dir_Len}}').format(Dir_Name= col,Dir_Len= Max_Dir_Len) + '|' + colored.green('{File_Name:{File_len}}').format(File_len= Max_file_Len,File_Name= col1)  + '|' )
                                if l==1: #print the lower part of header only once
                                    print('|' + Max_Dir_Len*'=' + '|' + Max_file_Len*'=' + '|')
                                    l=2
                            print('|' + Max_Dir_Len*'_' + '|' + Max_file_Len*'_' + '|')
                        elif args[0] == "rm":
                            if cwd=='':
                                dir = args[1]
                            else:
                                dir = full_dir + '/' + args[1]
                            if rm(master,dir):
                                master.rm(dir)
                        elif args[0] == "size":
                            Size(master,full_dir + '/' + args[1])
                        # Print last operation
                        elif args[0] == "last":
                            print(last)
                        # Dir operations
                        elif args[0] == "mkdir":
                            if args[1][0:1] == '/':
                                logging.info('Forbidden character!')
                                print('Forbidden character!')
                            else:
                                master.mkdir(args[1], full_dir)
                        elif args[0] == "rmdir":
                            dir_rmdir = full_dir + '/' + args[1]
                            master.rmdir(dir_rmdir)
                        elif args[0] == "get":
                            #get(master, args[1]) old without writing to file
                            dir_get = full_dir + '/' + args[1]
                            get(master, dir_get, args[2])
                        elif args[0] == "put":
                            if cwd == "":
                                put(master, source=args[1], filename=cwd + args[2])
                            else:
                                put(master, source=args[1], filename=full_dir + "/" + args[2])
                        elif args[0] == "cd":
                            if args[1:] and args[1] == "../" and cwd != '': #check if 2nd arg is exist and its ../
                                cwd = prev_dirc
                            else:
                                if cwd=='':
                                    full_dir = args[1]
                                elif args[1] == '/' or args[1] == ' ':
                                    full_dir = ''
                                else:
                                    full_dir = cwd + '/' + args[1]
                                print(full_dir)
                                if master.cd(full_dir):
                                    prev_dirc = cwd # previous dirc for ls ../ and cd ../
                                    if args[1] == '/':
                                        cwd = ""
                                        full_dir=''
                                    else:
                                        cwd = args[1]
                        elif args[0] == "exit":
                            logging.info('Client is closed')
                            sys.exit()
                    except IndexError:
                        logging.info('Wrong operation arguments')
                        print('Wrong operation arguments')
                except Exception as err:
                    print(err)
                    break
        except ConnectionError:
            logging.info('NS is not available now, please wait :)')
            print("NS is not available now, please wait :)")
        time.sleep(5)
if __name__ == "__main__":
    #main(sys.argv[1:])
    main()
