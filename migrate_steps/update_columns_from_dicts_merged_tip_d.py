"""
    This script insert into specified column real value from dictionary.
    Uses for swapping values for column ULICA and etc

    This script update column tip_d in merged table by values from sprsz_all
"""

import sys

import dbf
import datetime
import psycopg2


spr_table_name = 'sprsz_all'#'sprul_new_50'
table_to_mute = 'merged'#'g_osn_new_50'
column_to_mute = 'tip_d'#'ulica'

saved_spr = {}

def prepare_dict(conn):
    cur = conn.cursor()
    # This SQL for sprul - select count(*) from {0};
    # This SQL for sprsz - select count(*) from {0} where pr = 'tip_d';
    cur.execute("select count(*) from {0} where pr = 'tip_d';".format(spr_table_name))
    len = cur.fetchone()[0]
    cur.execute("select cod, ras from {0} where pr = 'tip_d';".format(spr_table_name))
    for i in range(len):
        line = cur.fetchone()
        # Next line for sprul, where we need cut off first 0 to get values 001, 002 etc.
        # saved_spr[line[0][1:]] = line[1]#.decode('utf-8')
        print line[0],line[1].decode('utf-8')
        saved_spr[line[0][:1]] = line[1].decode('utf-8')

    return saved_spr


def get_update_sql(cur):
    """ Generator that return id for update"""
    # select count(*) from (select distinct tip_d from merged) as f
    # "select count(*) from {0};
    cur.execute("select count(*) from (select distinct {0} from {1}) as f;".format(column_to_mute, table_to_mute))
    len = cur.fetchone()[0]

    cur.execute("select distinct {0} from {1};".format(column_to_mute, table_to_mute))

    for i in range(len):
        if not i % 1000:
            print i
        yield cur.fetchone()[0]

def main():
    conn_string = "host='localhost' dbname='soc' user='postgres' password='postgres'"
    conn = psycopg2.connect(conn_string)
    prepare_dict(conn)

    change_col_type(conn)

    # update values
    print "Prepare update SQL to execute..."
    sql = []#""
    for id in get_update_sql(conn.cursor()):
        sql.append("UPDATE {0} SET {1}='{2}' where {1}='{3}';\n".format(table_to_mute, column_to_mute, saved_spr[id].encode('utf-8') if id != '   ' else 'null', id))
        #sql = "UPDATE g_osn_new_50 SET ulica=%s where ulica=%s;\n"#.format("%s", "%s", "%s")
        #conn.cursor().execute(sql, (saved_spr[id] if id != '   ' else 'null', id))

    print "HERE WE SHOULD MAKE SQL EXEC"
    for i, line in enumerate(sql):
        if not i % 100:
            print i
        try:
            print "Executing \n", line
            conn.cursor().execute(line)
        except Exception:
            print line
            print Exception.message
            sys.exit(1)

    conn.commit()

    conn.close()

def change_col_type(conn):
    # change column type to varying(100)
    sql = "ALTER TABLE {0} ALTER COLUMN {1} TYPE character VARYING (100);".format(table_to_mute, column_to_mute)
    conn.cursor().execute(sql)

if __name__ == "__main__":
    main()