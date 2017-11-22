import rpyc
import sys
import os


def send_to_ds(data, data_servers):
    print("sending: " + str(data_servers))
    data_server = data_servers[0]
    data_servers = data_servers[1:]
    host, port = data_server

    con = rpyc.connect(host, port=port)
    data_server = con.root.Minion()
    data_server.put(data, data_servers)


def read_from_ds(file_path, data_server):
    host, port = data_server
    con = rpyc.connect(host, port=port)
    data_server = con.root.DataServer()
    return data_server.get(file_path)


# We always need to specify FULL FILE PATH to this function
def get(name_server, file_name):
    file_table = name_server.get_file_table_entry(file_name)
    if not file_table:
        print("404: file not found")
        return
    # Here it cycles over different DS and checks for blocks on each one. Since we don;t
    # have blocks anymore, this has to request full file path.
    for block in file_table:
        for m in [name_server.get_data_servers()[_] for _ in block[1]]:
            data = read_from_ds(block[0], m)
            if data:
                sys.stdout.write(data)
                break
        else:
            print("No blocks found. Possibly a corrupt file")


def put(name_server, source, dest):
    size = os.path.getsize(source)
    blocks = name_server.write(dest, size)
    with open(source) as f:
        for b in blocks:
            data = f.read(name_server.get_block_size())
            block_uuid = b[0]
            data_servers = [name_server.get_data_servers()[_] for _ in b[1]]
            send_to_ds(data, data_servers)


def main(args):
    """Add read/write/delete/size console commands"""
    con = rpyc.connect("localhost", port=2131)
    master = con.root.Master()

    if args[0] == "get":
        get(master, args[1])
    elif args[0] == "put":
        put(master, args[1], args[2])
    else:
        print("try 'put srcFile destFile OR get file'")

    # while True:
    #     # TODO: Need to parse input string into array
    #     str = input("user#:")
    #     cwd = ""
    #
    #     # Current working dir
    #     if str == "cwd":
    #         pass
    #
    #     # File operations
    #     if str == "touch":
    #         pass
    #     if str == "ls":
    #         pass
    #     if str == "rm":
    #         pass
    #     if str == "size":
    #         pass
    #
    #     # Last operation
    #     if str == "last":
    #         pass
    #
    #     # Dir operations
    #     if str == "mkdir":
    #         pass
    #     if str == "rmdir":
    #         pass
    #     if str == "listdir":
    #         pass

if __name__ == "__main__":
    main(sys.argv[1:])
    # main()
