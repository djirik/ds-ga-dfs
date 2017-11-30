from functools import reduce
import operator
d = {'dfs.conf': 'file', 'aaa': 'file', 'sfdsf': 'file', 'asa': 'file', 'ssss': 'file', 'test': 'file', 'dir1': {}}
path = 'dir1'
dir_name = 'dir2'

map_list = path.split('/')
dir_to_add = {dir_name: {}}

if path == '':
    d.update(dir_to_add)
else:
    reduce(operator.getitem, map_list, d).update(dir_to_add)
print(d)
