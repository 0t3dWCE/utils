#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    This script insert into specified column real value from dictionary.
    Uses for swapping values for column ULICA and etc

    This script update column k_uvd in merged table by values from sprps_50 and sprps_56
"""

import sys
import psycopg2


spr_table_names = ['sprps_50_new_delme', 'sprps_56_new_delme']#'sprsz_all'#'sprul_new_50'
table_to_mute = 'merged'#'g_osn_new_50'
column_to_mute = 'gor' #'tip_d'#'ulica'

saved_spr = {}

def prepare_dict(conn):
    cur = conn.cursor()
    # This SQL for sprul - select count(*) from {0};
    # This SQL for sprsz - select count(*) from {0} where pr = 'tip_d';
    for spr_table_name in spr_table_names:
        cur.execute("select count(*) from {0};".format(spr_table_name))
        len = cur.fetchone()[0]
        cur.execute("select cod_ps, name_ps from {0};".format(spr_table_name))
        for i in range(len):
            line = cur.fetchone()
            # Next line for sprul, where we need cut off first 0 to get values 001, 002 etc.
            # saved_spr[line[0][1:]] = line[1]#.decode('utf-8')
            if line[0][1:] not in saved_spr:
                print line[0][1:],line[1].decode('utf-8')
                saved_spr[line[0][1:]] = line[1].decode('utf-8')

    return saved_spr


def get_update_sql(cur):
    """ Generator that return id for update"""
    # select count(*) from (select distinct tip_d from merged) as f
    # "select count(*) from {0};"
    # we need count of distinct records because of next dist select
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
    sql = []
    # be shure that you find all invalid gor id in merged that doesn't presented in sprps_50/56
    # select distinct m.k_uvd from merged as m left join spruvd_all as s ON m.k_uvd = s.k_uvd Where s.k_uvd is null
    banned = ['   ']
    for id in get_update_sql(conn.cursor()):
        try:
            sql.append("UPDATE {0} SET {1}='{2}' where {1}='{3}';\n".format(table_to_mute, column_to_mute, saved_spr[id].encode('utf-8') if id not in banned else id, id))
        except KeyError:
            print "id is ", id
            sys.exit(1)

    print "HERE WE SHOULD MAKE SQL EXEC"
    for i, line in enumerate(sql):
        if not i % 100:
            print i
        try:
            print "Executing \n", line
            conn.cursor().execute(line)
        except Exception as e:
            print line
            print e.message
            sys.exit(1)

    conn.commit()

    conn.close()

def change_col_type(conn):
    # change column type to varying(50)
    sql = "ALTER TABLE {0} ALTER COLUMN {1} TYPE character VARYING (50);".format(table_to_mute, column_to_mute)
    conn.cursor().execute(sql)

if __name__ == "__main__":
    main()