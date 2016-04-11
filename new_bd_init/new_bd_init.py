"""
Usage: 
   new_bd_init.py --host=host --db=db --user=user --pwd=pwd --dir=dir [--drop] 

Options:
   --drop    Drop all tables from DB before DB INIT
"""

import psycopg2

from docopt import docopt
from os import listdir
from os.path import isfile, join
from pprint import pprint
from psycopg2 import IntegrityError


def main(conn, dir):
	cur = conn.cursor()
	sqls = find_sql_files_in_dir(dir)
	for file in sorted(sqls):
		print dir + file
		# We need remove BOM symbols via 'utf-8-sig'
		script = open(dir + file, 'r').read().decode('utf-8-sig')
		try:
			cur.execute(script)
			conn.commit()
		except IntegrityError as e:
			print "\n Can't execute ", dir + file
                        print e.message
			break
	print "DONE"
		
def find_sql_files_in_dir(dir):
	onlysqlfiles = [ f for f in listdir(dir) if isfile(join(dir,f)) and ".sql" in f]
	return onlysqlfiles

def drop_all_tables(conn):
	cur = conn.cursor()
	cur.execute("drop schema public cascade;") 
	cur.execute("create schema public;")
	conn.commit()

def init():
	args = docopt(__doc__)
        print "args", args
	try:
            conn = psycopg2.connect(host=args["--host"], database=args["--db"], user=args["--user"], password=args["--pwd"])
	except Exception as e:	
            print "Unable to connect...", e.message
	
	if args["--drop"]:
		drop_all_tables(conn)
	
	return conn, args["--dir"]

if __name__ == "__main__":
	conn, dir = init()
	main(conn, dir)
