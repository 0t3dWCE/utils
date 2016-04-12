#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
print sys.getdefaultencoding()

import dbf
import datetime
import psycopg2
import itertools

class dbf2pg():
    '''This script create copy of DBF table in Postgres'''

    def __init__(self):
        self.dbf_file = u''
        self.pg_table_name = u''
        self.camposValue = u''
        self.start_time = datetime.datetime.now()

    def pg_connect(self, stringConnect):
        try:
            self.conn = psycopg2.connect(stringConnect)
            self.cur = self.conn.cursor()
        except:
            exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
            sys.exit("Database connection failed!\n ->%s" % (exceptionValue))

    def pg_execute_sql(self, sql_cmd):
        try:
            self.cur.execute(sql_cmd)
        except:
            exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
            print "\n Invalid SQL command \n", sql_cmd
            sys.exit("pg_execute_sql Operation failed!\n ->%s\n%s" % (exceptionValue, exceptionTraceback))

    def open_dbf(self):
        self.dbf_table = dbf.Table(self.dbf_file, codepage='cp866')
        self.dbf_table.open()

    def create_pg_table(self):
        ''' Get DBF fileds typesa and prepare sql command for table creation in Postgres'''

        fields = ''
        field_type =''
        for filed in self.dbf_table.field_names:
            #Get fields types - prepare the fields to make the "Create Table"
            type, long, long2, tipo2 = self.dbf_table.field_info(filed)
            if type == 'C':
                field_type = '{0} character varying({1}) ,'.format(filed, long)
            elif type == 'D':
                field_type = '{0} date ,'.format(filed)
            elif type == 'M':
                field_type = '{0} text ,'.format(filed)
            elif type == 'L':
                field_type = '{0} boolean ,'.format(filed)
            elif type == 'T':
                field_type = '{0} timestamp without time zone ,'.format(filed)
            elif type == 'I':
                field_type = '{0} integer ,'.format(filed)
            elif type == 'N':
                field_type = '{0} numeric({1},{2}) ,'.format(filed, long, long2)
            fields = fields + field_type

        create_table_sql_cmd = 'CREATE TABLE {0} ({1})'.format(self.pg_table_name, fields[:-1].strip())
        return create_table_sql_cmd

    def create_columns(self, columns):
        conn = psycopg2.connect("host='localhost' dbname='soc' user='postgres' password='postgres'")
        cur = conn.cursor()
        print columns
        for col in columns:
            cur.execute('ALTER TABLE "g_osn" ADD COLUMN {0} text;'.format(col))
        conn.commit()

    def prepare_insert_lines(self):
        '''Generator which prepare sql command for insert line to Postgres'''

        separator = u','
        camposValue = separator.join(self.dbf_table.field_names)
        for i, record in enumerate(self.dbf_table):
            if not i % 1000:
                print i
            x = [field for field in record]
            sqlInsert = 'insert into {0} ({1}) values ({2})'.format(self.pg_table_name, camposValue, ", ".join(["%s"] * len(x)))
            yield sqlInsert, x


    def process(self, create_table_func='', insert_to_pg_func=''):
        ''' '''
        self.create_pg_table = create_table_func
        self.insert_to_pg = insert_to_pg_func

        if self.create_pg_table or self.insert_to_pg:
            conn_string = "host='localhost' dbname='soc' user='postgres' password='postgres'"
            self.pg_connect(conn_string)

            if self.create_pg_table:
                self.pg_execute_sql(self.create_pg_table)

            if self.insert_to_pg:
                for insert_line in self.insert_to_pg:
                    # This is slim moment - psycopg2 do insert correctly with two params
                    # one of them - insert command, second is values
                    self.cur.execute(insert_line[0], insert_line[1])
            self.conn.commit()
            self.conn.close()

if __name__ == '__main__':
    app = dbf2pg()

    #app.dbf_file = '/home/ayakimov/Soc/50/1115/Z_UDS.DBF'
    #app.dbf_file = '/home/ayakimov/Soc/SPRSZ.DBF'
    app.dbf_file = '/home/ayakimov/Soc/Dicts/56/SPRPS.DBF'
    app.open_dbf()

    app.pg_table_name = 'SPRPS_56_new_delme'

    x = app.create_pg_table()
    y = app.prepare_insert_lines()
    app.process(x, y)

    end_time = datetime.datetime.now()
    total_time = end_time - app.start_time
    print 'Total seconds elapsed:{0}'.format(total_time.seconds)
