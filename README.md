# ds-ga-dfs

#file table structure:
It is nested dictionary where files are defined as dictionary with single `key:value` pair with `value = 'file'` e.g.
`file = { 'test.txt': 'file' }`.

Directory is defined as dictionary with single `key:value` pair where `value = {}` (another dictionary) e.g.
 `dir = { 'root' : { 'test.txt' : 'file'}}`
 
File system structure is defined as a combination of these both definitions with starting directory `root`