'''
Created on 1 apr. 2019

@author: thomasgumbricht
'''

from sys import exit
from os import path, makedirs, system, listdir
from geoimagine.support import karttur_dt as kt_dt
from subprocess import PIPE, Popen


class ProcessSqlDumps:
    '''
    '''   

    def __init__(self, process, session, verbose):
        #self.session = session
        self.verbose = verbose
        self.process = process   
        self.session = session         
        #direct to subprocess
        #direct to subprocess
        
        if self.process.proc.processid == 'exporttabledatacsvsql':
            self.ExportTableDataCsvSqlIni()
        elif self.process.proc.processid == 'dumptablesql':
            self.DumpExportTableIni()
        elif self.process.proc.processid == 'dumpschemasql':
            self.DumpExportSchemaIni()
        elif self.process.proc.processid == 'dumpdbsql':
            self.DumpExportDB()
        elif self.process.proc.processid == 'exportalldatacsvsql':
            self.ExportAllDataCsvSql()
        elif self.process.proc.processid == 'copyalldatacsvsql':
            self.CopyAllDataCsvSql()
        elif self.process.proc.processid == 'copytabledatacsvsql':          
            self.CopyTableCsvSqlIni()
        elif self.process.proc.processid == 'restoretablesql':  
            self.RestoreTableIni()
        elif self.process.proc.processid == 'restoreschemasql':  
            self.RestoreSchemaIni()
        elif self.process.proc.processid == 'restoredbsql':  
            self.RestoreDB()
            
        elif self.process.proc.processid == 'dumptablecompletesql':
            self.DumpTableCompleteSql()
        elif self.process.proc.processid == 'restoretablecompletesql':
            self.RestoreTableCompleteSql()
        else:
            exitstr = 'Unrecognized process in ProcessSqlDumps: %s' %(self.process.processid)
            exit(exitstr)
            
    def ExportAllDataCsvSql(self):
        '''
        '''
        schemaL = self.session._SeleatAllSchema()
        for schema in schemaL:
            print ('    dumping',schema)
            tableL = self.session._SelectAllSchemaTables(schema)
            for table in tableL:
                print ('        table',table)
                self.ExportTableDataCsvSql(schema,table)
         
    def ExportTableDataCsvSqlIni(self):
        self.ExportTableDataCsvSql(self.process.params.schema,self.process.params.table)
                
    def ExportTableDataCsvSql(self, schema, table):
        import csv
        schematab = '%s.%s' %(schema,table)
        if self.process.dstpath.volume.lower() == 'none':
            vol =''
        else:
            vol = '/volumes/%s' %(self.process.dstpath.volume)
        FP = path.join(vol,'SQLdump',schema,table)
        today = kt_dt.DateToStrDate(kt_dt.Today())
        FN = '%s_sqldump_%s.csv' %(table, today)
        csvFPN = path.join(FP,FN)
        if not path.exists(FP):
            makedirs(FP)
            
        if path.isfile(csvFPN) and not self.process.overwrite:
            return
        #Get the columns
        tableColumnDefL = self.session._GetTableColumns(schema,table)
        tableColNameL = [row[0] for row in tableColumnDefL]

        query = {'schematab':schematab, 'items': ",".join(tableColNameL)}
        #Get all the data in the table
        print ("SELECT %(items)s FROM %(schematab)s;" %query)
        recs = self.session._SelectAllTableRecs(query)
        #self.cursor.execute("SELECT %(items)s FROM %(schematab)s;" %query)
        #records = self.cursor.fetchall()
        if len(recs) == 0:
            warnstr ='        WARNING, empty sql dump: skipping export to %s' %(csvFPN)
            print (warnstr)
            return

        #open csv file for writing
        print ('    Dumping db records to',csvFPN)
        F = open(csvFPN,'w')
        wr = csv.writer(F, delimiter =";")
        wr.writerow(tableColNameL)
        for row in recs:
            wr.writerow(row)
        F.close()
        
    def DumpExportTableIni(self):
        self.DumpExportTable(self.process.params.schema,self.process.params.table)
        
    def DumpExportTable(self, schema, table):
        #(host_name,database_name,user_name,database_password):
        if self.process.params.dataonly and self.process.params.schemaonly:
            exit('BOTh dataonly and schemaonly can not be set to TRUE')
        schematab = '%s.%s' %(schema,table)
        username, account, password = self.session._SelectUserSecrets()
        
        if self.process.dstpath.volume.lower() == 'none':
            vol =''
        else:
            vol = '/volumes/%s' %(self.process.dstpath.volume)
        FP = path.join(vol,'SQLdump',schema,table)
        today = kt_dt.DateToStrDate(kt_dt.Today())
        #Set filename
        #FN = '%s_sqldump_%s' %(table, today)
        FN = '%s_sqldump-%s' %(table, self.process.params.format)
        if self.process.params.dataonly:
            FN += '-dataonly'
        elif self.process.params.schemaonly:
            FN += '-schemaonly'
   
        FN += '_%s.sql' %(today)
        
        sqlFPN = path.join(FP,FN)
        if not path.exists(FP):
            makedirs(FP)
            
        if path.isfile(sqlFPN) and not self.process.overwrite:
            return

        host = 'localhost'
        db = 'postgres'
        
        #pg_dump -h localhost -p 5432 -U karttur -t process.subprocesses -f dbtab.sql postgres
        '''
        cmd = 'pg_dump -h {0} -p 5432 -U {1} -t {2} -Fc -f {3} {4}'\
        .format(host,username,schematab,sqlFPN,db)
        #print (cmd)
        #BALLE
        #oscmd = '%(cmdpath)s  karttur -U postgres -t %(schematab)s -F c > %(sqlfpn)s' %{'cmdpath':cmdpath,'schematab':schematab, 'sqlfpn':sqlFPN}
        #print (oscmd)
        #p = Popen(cmd,shell=True,stdin=PIPE,stdout=PIPE,stderr=PIPE)
        #p = Popen(cmd,stdin=PIPE,stdout=PIPE,stderr=PIPE)
        proc = Popen(['pg_dump', '-h', host, '-U', username, '-p', '5432',
                           '-t', schematab, '-f', sqlFPN,  db],
                            shell=True, stdin=PIPE)
        proc.wait()

        proc = Popen(['pg_dump', '-h', host, '-U', username, '-p', 5432,
                           '-F', 't', '-f', sqlFPN,  db],
                            cwd=directory, shell=True, stdin=PIPE)

        print ('old',cmd)
        '''
        if self.process.params.cmdpath == 'None':
            cmd = 'pg_dump'
        else:
            #print (self.process.params.cmdpath)
            cmd = '%s/pg_dump' %( self.process.params.cmdpath )
        if self.process.params.dataonly:
            cmd += ' -h {0} -p 5432 -U {1} -t {2} -F {3} -f {4} -a {5}'\
            .format(host,username,schematab,self.process.params.format,sqlFPN,db)
        elif self.process.params.schemaonly:
            cmd += ' -h {0} -p 5432 -U {1} -t {2} -F {3} -f {4} -s {5}'\
            .format(host,username,schematab,self.process.params.format,sqlFPN,db)
        else:
            cmd += ' -h {0} -p 5432 -U {1} -t {2} -F {3} -f {4} {5}'\
            .format(host,username,schematab,self.process.params.format,sqlFPN,db)

        system(cmd)

        #return p.communicate('{}\n'.format(database_password))
        
    def DumpExportSchemaIni(self):
        self.DumpExportSchema(self.process.params.schema)  
      
    def DumpExportSchema(self, schema):
        #(host_name,database_name,user_name,database_password):
        if self.process.params.dataonly and self.process.params.schemaonly:
            exit('BOTh dataonly and schemaonly can not be set to TRUE')

        username, account, password = self.session._SelectUserSecrets()
        
        if self.process.dstpath.volume.lower() == 'none':
            vol =''
        else:
            vol = '/volumes/%s' %(self.process.dstpath.volume)
        FP = path.join(vol,'SQLdump',schema)
        today = kt_dt.DateToStrDate(kt_dt.Today())
        #Set filename

        FN = '%s_sqldump-%s' %(schema, self.process.params.format)
        if self.process.params.dataonly:
            FN += '-dataonly'
        elif self.process.params.schemaonly:
            FN += '-schemaonly'
   
        FN += '_%s.sql' %(today)
        
        sqlFPN = path.join(FP,FN)
        if not path.exists(FP):
            makedirs(FP)
            
        if path.isfile(sqlFPN) and not self.process.overwrite:
            return

        host = 'localhost'
        db = 'postgres'

        if self.process.params.cmdpath == 'None':
            cmd = 'pg_dump'
        else:
            cmd = '%s/pg_dump' %( self.process.params.cmdpath )
        if self.process.params.dataonly:
            cmd += ' -h {0} -p 5432 -U {1} -n {2} -F {3} -f {4} -a {5}'\
            .format(host,username,schema,self.process.params.format,sqlFPN,db)
        elif self.process.params.schemaonly:
            cmd += ' -h {0} -p 5432 -U {1} -n {2} -F {3} -f {4} -s {5}'\
            .format(host,username,schema,self.process.params.format,sqlFPN,db)
        else:
            cmd += ' -h {0} -p 5432 -U {1} -n {2} -F {3} -f {4} {5}'\
            .format(host,username,schema,self.process.params.format,sqlFPN,db)

        system(cmd)
         
    def DumpExportDB(self):
        #(host_name,database_name,user_name,database_password):
        if self.process.params.dataonly and self.process.params.schemaonly:
            exit('BOTh dataonly and schemaonly can not be set to TRUE')

        username, account, password = self.session._SelectUserSecrets()
        
        if self.process.dstpath.volume.lower() == 'none':
            vol =''
        else:
            vol = '/volumes/%s' %(self.process.dstpath.volume)
        FP = path.join(vol,'SQLdump')
        today = kt_dt.DateToStrDate(kt_dt.Today())
        #Set filename

        FN = 'sqldump-%s' %(self.process.params.format)
        if self.process.params.dataonly:
            FN += '-dataonly'
        elif self.process.params.schemaonly:
            FN += '-schemaonly'
   
        FN += '_%s.sql' %(today)
        
        sqlFPN = path.join(FP,FN)
        if not path.exists(FP):
            makedirs(FP)
            
        if path.isfile(sqlFPN) and not self.process.overwrite:
            return

        host = 'localhost'
        db = 'postgres'

        if self.process.params.cmdpath == 'None':
            cmd = 'pg_dump'
        else:
            cmd = '%s/pg_dump' %( self.process.params.cmdpath )
        if self.process.params.dataonly:
            cmd += ' -h {0} -p 5432 -U {1} -F {2} -f {3} -a {4}'\
            .format(host,username,self.process.params.format,sqlFPN,db)
        elif self.process.params.schemaonly:
            cmd += ' -h {0} -p 5432 -U {1} -F {2} -f {3} -s {4}'\
            .format(host,username,self.process.params.format,sqlFPN,db)
        else:
            cmd += ' -h {0} -p 5432 -U {1} -F {2} -f {3} {4}'\
            .format(host,username,self.process.params.format,sqlFPN,db)

        system(cmd)
        
    def CopyAllDataCsvSql(self):
        schemaL = self.session._SeleatAllSchema()
        for schema in schemaL:
            print ('    dumping',schema)
            tableL = self.session._SelectAllSchemaTables(schema)
            for table in tableL:
                print ('        table',table)
                nrrecs = self.session._SelectCount(schema, table)
                if self.process.overwrite or nrrecs == 0:
                    self.CopyTablCsvSql(schema, table)
                else:
                    self.InsertTablCsvSql(schema, table)
                    
                    
                
    def CopyTableCsvSqlIni(self):
        schema = self.process.params.schema
        table =  self.process.params.table  
        print ('        table',table)
        nrrecs = self.session._SelectCount(schema, table)
        if self.process.overwrite or nrrecs == 0:
            self.CopyTablCsvSql(schema, table)
        else:
            self.InsertTableCsvSql(schema, table) 
                        
               
    def CopyTableCsvSql(self,schema,table):
        import csv

        printstr = 'copying data to schema: %s; table: %s' %(schema, table)
        print (printstr)
        
        if self.process.srcpath.volume.lower() == 'none':
            vol =''
        else:
            vol = '/volumes/%s' %(self.process.srcpath.volume)
        FP = path.join(vol,'SQLdump',schema,table)
        #today = kt_dt.DateToStrDate(kt_dt.Today())
        #Set filename
        #FN = '%s_sqldump_%s' %(table, today)
        FN = '%s_sqldump' %(table)

        
        ext = '.csv'
        FN = self._GetLatestDump(FP,FN,ext)
        if not FN:
            return
        
        csvFPN = path.join(FP,FN)
        if not path.exists(csvFPN):
            exit('sql file does not exists', csvFPN)
            
        with open(csvFPN) as f:
            reader = csv.reader(f, delimiter =";")
            headerL = next(reader)
        tab = '%(schema)s.%(tab)s' %{'schema':schema,'tab':table}
        query = {'tab':tab,'items': ",".join(headerL), 'csv':csvFPN}
        print ("COPY %(tab)s (%(items)s) FROM '%(csv)s' WITH CSV HEADER DELIMITER ',';" %query)
        BALLE
        self.cursor.execute("COPY %(tab)s (%(items)s) FROM '%(csv)s' WITH CSV HEADER DELIMITER ';';" %query)
        self.conn.commit()
        
    def InsertTableCsvSql(self,schema,table):
        import csv

        printstr = 'inserting data to schema: %s; table: %s' %(schema, table)
        print (printstr)
        
        if self.process.srcpath.volume.lower() == 'none':
            vol =''
        else:
            vol = '/volumes/%s' %(self.process.srcpath.volume)
        FP = path.join(vol,'SQLdump',schema,table)
        #Set filename
        FN = '%s_sqldump' %(table)

        ext = '.csv'
        FN = self._GetLatestDump(FP,FN,ext)
        if not FN:
            return
        
        csvFPN = path.join(FP,FN)
        if not path.exists(csvFPN):
            exit('sql file does not exists', csvFPN)
        schematab = '%(schema)s.%(tab)s' %{'schema':schema,'tab':table}
        #Get the key columns
        tabkeysL = self.session._GetTableKeys(schema,table)
        tabkeysL = [item[0] for item in tabkeysL]
        
        with open(csvFPN) as f:
            reader = csv.reader(f, delimiter =";")
            headerL = next(reader)
            keyitemL = []
            for key in tabkeysL:
                keyitemL.append(headerL.index(key))

            for row in reader:
                queryD = {}
                for i,key in enumerate(tabkeysL):
                    queryD[key] = row[keyitemL[i]]

                rec = self.session._SingleSearch(queryD, tabkeysL,schema,table)
                if rec == None:
                    values =["'{}'".format(str(x)) for x in row]
                    query = {'schematab':schematab, 'cols':",".join(headerL), 'values':",".join(values)}
                    self.session._InsertQuery(query)
                    
               
    def test(self):
        cols =  ','.join(paramL)
        selectQuery = {'schema':schema, 'table':table, 'select': wherestatement, 'cols':cols}
        
        query = "SELECT %(cols)s FROM %(schema)s.%(table)s %(select)s" %selectQuery

        self.cursor.execute(query)
        self.records = self.cursor.fetchone()
        return self.records

    def RestoreTableIni(self):
        self.RestoreTable(self.process.params.schema,self.process.params.table)
        
    def RestoreTable(self, schema, table):
        #(host_name,database_name,user_name,database_password):
        if self.process.params.dataonly and self.process.params.schemaonly:
            exit('BOTh dataonly and schemaonly can not be set to TRUE')
        schematab = '%s.%s' %(schema,table)
        username, account, password = self.session._SelectUserSecrets()
        
        if self.process.dstpath.volume.lower() == 'none':
            vol =''
        else:
            vol = '/volumes/%s' %(self.process.dstpath.volume)
        FP = path.join(vol,'SQLdump',schema,table)
        #today = kt_dt.DateToStrDate(kt_dt.Today())
        #Set filename
        #FN = '%s_sqldump_%s' %(table, today)
        FN = '%s_sqldump-%s' %(table, self.process.params.format)
        if self.process.params.dataonly:
            FN += '-dataonly'
        elif self.process.params.schemaonly:
            FN += '-schemaonly'
        print ('FN',FN)
        
        ext = '.sql'
        FN = self._GetLatestDump(FP,FN,ext)
        if not FN:
            return
        
        sqlFPN = path.join(FP,FN)
        if not path.exists(sqlFPN):
            exit('sql file does not exists', sqlFPN)
            
        host = 'localhost'
        db = 'postgres'
        
        if self.process.params.cmdpath == 'None':
            cmd = 'pg_restore'
        else:
            #print (self.process.params.cmdpath)
            cmd = '%s/pg_restore' %( self.process.params.cmdpath )
        if self.process.params.dataonly:
            cmd += ' -h {0} -p 5432 -U {1} -t {2} -a -d {3} {4}'\
            .format(host,username,schematab,db,sqlFPN)
        elif self.process.params.schemaonly:
            cmd += ' -h {0} -p 5432 -U {1} -t {2} -s -d {3} {4}'\
            .format(host,username,schematab,db,sqlFPN)
        else:
            cmd += ' -h {0} -p 5432 -U {1} -t {2} -d {3} {4}'\
            .format(host,username,schematab,db,sqlFPN)
        print (cmd)
        BALLE
        system(cmd)
       
    def RestoreSchemaIni(self):
        self.RestoreTable(self.process.params.schema) 
        
    def RestoreSchema(self, schema):
        #(host_name,database_name,user_name,database_password):
        if self.process.params.dataonly and self.process.params.schemaonly:
            exit('BOTh dataonly and schemaonly can not be set to TRUE')
        username, account, password = self.session._SelectUserSecrets()
        
        if self.process.dstpath.volume.lower() == 'none':
            vol =''
        else:
            vol = '/volumes/%s' %(self.process.dstpath.volume)
        FP = path.join(vol,'SQLdump',schema)
        #today = kt_dt.DateToStrDate(kt_dt.Today())
        #Set filename
        #FN = '%s_sqldump_%s' %(table, today)
        FN = '%s_sqldump-%s' %(schema, self.process.params.format)
        if self.process.params.dataonly:
            FN += '-dataonly'
        elif self.process.params.schemaonly:
            FN += '-schemaonly'
        print ('FN',FN)
        
        ext = '.sql'
        FN = self._GetLatestDump(FP,FN,ext)
        if not FN:
            return
        
        sqlFPN = path.join(FP,FN)
        if not path.exists(sqlFPN):
            exit('sql file does not exists', sqlFPN)
            
        host = 'localhost'
        db = 'postgres'
        
        if self.process.params.cmdpath == 'None':
            cmd = 'pg_restore'
        else:
            #print (self.process.params.cmdpath)
            cmd = '%s/pg_restore' %( self.process.params.cmdpath )
        if self.process.params.dataonly:
            cmd += ' -h {0} -p 5432 -U {1} -n {2} -a -d {3} {4}'\
            .format(host,username,schema,db,sqlFPN)
        elif self.process.params.schemaonly:
            cmd += ' -h {0} -p 5432 -U {1} -n {2} -s -d {3} {4}'\
            .format(host,username,schema,db,sqlFPN)
        else:
            cmd += ' -h {0} -p 5432 -U {1} -n {2} -d {3} {4}'\
            .format(host,username,schema,db,sqlFPN)
        print (cmd)
        BALLE
        system(cmd)
        
    def RestoreDB(self):
        #(host_name,database_name,user_name,database_password):
        if self.process.params.dataonly and self.process.params.schemaonly:
            exit('BOTh dataonly and schemaonly can not be set to TRUE')

        username, account, password = self.session._SelectUserSecrets()
        
        if self.process.dstpath.volume.lower() == 'none':
            vol =''
        else:
            vol = '/volumes/%s' %(self.process.dstpath.volume)
        FP = path.join(vol,'SQLdump')

        FN = 'sqldump-%s' %(self.process.params.format)
        if self.process.params.dataonly:
            FN += '-dataonly'
        elif self.process.params.schemaonly:
            FN += '-schemaonly'

        ext = '.sql'
        FN = self._GetLatestDump(FP,FN,ext)
        if not FN:
            return
        
        sqlFPN = path.join(FP,FN)
        if not path.exists(sqlFPN):
            exit('sql file does not exists', sqlFPN)

        host = 'localhost'
        db = 'postgres'

        if self.process.params.cmdpath == 'None':
            cmd = 'pg_restore'
        else:
            #print (self.process.params.cmdpath)
            cmd = '%s/pg_restore' %( self.process.params.cmdpath )
        if self.process.params.dataonly:
            cmd += ' -h {0} -p 5432 -U {1} -a -d {2} {3}'\
            .format(host,username,db,sqlFPN)
        elif self.process.params.schemaonly:
            cmd += ' -h {0} -p 5432 -U {1} -s -d {2} {3}'\
            .format(host,username,db,sqlFPN)
        else:
            cmd += ' -h {0} -p 5432 -U {1} -d {2} {3}'\
            .format(host,username,db,sqlFPN)
        print (cmd)
        BALLE
        system(cmd)

        system(cmd)
    
    #if nrrecs > 0:
    #       print 'deleting records: %s; table: %s' %(process.parameters.schema, process.parameters.table)
    #       self.ConnCommon.DeleteAllRecords(process.parameters.schema, process.parameters.table)
            
    def _GetLatestDump(self,FP,FN,ext):
        #Get the files in the input mainpath
        FL = [ f for f in listdir(FP) if path.isfile(path.join(FP,f)) and FN in f and path.splitext(f)[1] == ext]
        if len(FL) == 0:
            print ('    no dump data to copy')
            return False
       
        datumL = []
        for FN in FL:
            datumL.append([path.splitext(FN.split('_')[2])[0],FN])
        if self.process.params.datum in datumL:
            index = datumL.index(self.process.params.datum)
            csvFN = datumL[index][1]
        else:
            #get the latest
            datumL.sort()
            datumL.reverse()
            csvFN = datumL[0][1]

        csvFPN = path.join(FP,csvFN)
        return (csvFPN)

