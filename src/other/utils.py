import os
import re
import subprocess
import duckdb
from subprocess import run



def run_db(db_name):
  con = duckdb.connect(database=':memory:')
  table_names = get_table_names(db_name)
  for table_name in table_names:
      bash_cmd = f'sqlite3 -header -csv {db_name} "SELECT * from {table_name};">{table_name}.csv'
      #bash_cmd = bash_cmd.split(' ')
      #print(bash_cmd)
      #proc = subprocess.Popen(bash_cmd, stdout=subprocess.PIPE)
      data = run(bash_cmd,capture_output=True,shell=True)
      con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{table_name}.csv');")
      #os.remove(f"{table_name}.csv")
  return con


def get_table_names(db_name):
    cmd = f'sqlite3 {db_name} .tables'
    cmd = cmd.split(' ')
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    a=proc.stdout.readlines()
    a=" ".join([x.decode("utf-8").replace('\n','')  for x in a])
    table_names = a.split(' ')
    table_names = [x for x in table_names if x]
    return table_names


  
db_files = {'geo':'data/geography-db.added-in-2020.sqlite',
            'world_1':'spider/database/world_1/world_1.sqlite',
            'flight_4':'spider/database/flight_4/flight_4.sqlite',
            'flight_2':'spider/database/flight_2/flight_2.sqlite',
            'singer':'spider/database/singer/singer.sqlite'
            }


def adjust_nodes_old(nodes):

    # remove projection after aggregation
    fix_nodes=[]
    for n in nodes:
        #print(n)
        if n[0]=='PROJECTION' and fix_nodes and fix_nodes[-1][0]=='AGGREGATE' and n[1:]==fix_nodes[-1][1:]: continue
        fix_nodes.append(n)
    

    # split multiple attributes
    adjusted_nodes=[]
    for n in fix_nodes:
        #print(n)
        if n[0] in {'FILTER','PROJECTION','AGGREGATE'} and len(n)>2:
            adjusted_filters = [[n[0],x] for x in n[1:]]
            adjusted_nodes.extend(adjusted_filters)
        else:   
            adjusted_nodes.append(n)


    # split aggregates
    # ['AGGREGATE', 'sum(SurfaceArea)']  ==> ['AGGREGATE_PROJ', 'SurfaceArea','AGGREGATE_OP', 'SurfaceArea']
    
    adjusted_nodes2=[]

    for n in adjusted_nodes:
        if n[0]=='AGGREGATE' and n[1]!='count_star()':
            func, arg = re.findall("([a-zA-Z_]+)\((.*)\)",n[1])[0]
            adjusted_nodes2.extend([['AGGREGATE_PROJ',arg],['AGGREGATE_OP',func]])

        else: adjusted_nodes2.append(n)

    return adjusted_nodes2

# # tree_nodes = adjust_nodes(flattened_tree_nodes)
# # tree_nodes = list(dict.fromkeys(["_".join(x) for x in tree_nodes]))
# # tree_nodes_map = {k:"" for k in tree_nodes}


def adjust_nodes(node):

    if node.op=='JOIN': 
        node.adjusted_nodes = ['JOIN']
        return 
    # split multiple attributes
    adjusted_nodes=[]
    if node.op in {'FILTER','PROJECTION','AGGREGATE'} and len(node.args)>1:
        adjusted_nodes = [[node.op,x] for x in node.args]
    else:   
        adjusted_nodes = [node.text]


    # split aggregates
    # ['AGGREGATE', 'sum(SurfaceArea)']  ==> ['AGGREGATE_PROJ', 'SurfaceArea','AGGREGATE_OP', 'SurfaceArea']
    
    adjusted_nodes2=[]

    for n in adjusted_nodes:
        if n[0]=='AGGREGATE' and n[1]!='count_star()':
            func, arg = re.findall("([a-zA-Z_]+)\((.*)\)",n[1])[0]
            adjusted_nodes2.extend([['AGGREGATE_PROJ',arg],['AGGREGATE_OP',func]])

        else: adjusted_nodes2.append(n)

    node.adjusted_nodes = adjusted_nodes2
# tree_nodes = adjust_nodes(flattened_tree_nodes)
# tree_nodes = list(dict.fromkeys(["_".join(x) for x in tree_nodes]))
# tree_nodes_map = {k:"" for k in tree_nodes}


def augment_questions(question_dict):
    d={}
    for k in question_dict:
        question = question_dict[k]
        first_word = question.split(' ')[0]
        if first_word in {'Is','Does'}: question +=' Answer with Yes or No only.'
        if first_word =='List': question +=' Separate them by a comma. List as much as you can.'
        d[k] = question
    return d


def tree_adjust_nodes(head):
    if head and head.text:
        adjust_nodes(head)
        tree_adjust_nodes(head.l)
        tree_adjust_nodes(head.r)


def replace_units(x):
    x=x.lower()
    return x.replace('thousand','*10**3').replace('million','*10**6').replace('billion','*10**9').replace('trillion','*10**12')

import numpy as np

def map_func(func):
    if func=='sum': return np.sum
    if func=='avg': return np.mean
    if func=='max': return np.max
    if func == 'count': return len