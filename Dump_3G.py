import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
from datetime import date
import re
import datetime
import ImportDF


def processArchive():
    pathImport = '/import/Dump_3G'
    pathImportSI = os.getcwd() + pathImport
    #print (pathImportSI)
    archiveName = pathImport[8:len(pathImport)]
    #print (archiveName)
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    #print ('loalding files...\n')
    all_filesSI = glob.glob(pathImportSI + "/*.csv")
    all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    DateCreation = datetime.datetime.fromtimestamp(os.path.getmtime(all_filesSI[0])).strftime("%Y%m%d")
    csv_path = os.path.join(script_dir, 'export/Dump_3G/'+DateCreation+'_'+archiveName+'.csv')
    #print (all_filesSI)
    li = []
    
    for filename in all_filesSI:
        dataArchive = filename[len(pathImportSI)+9:len(filename)-4]
        iter_csv = pd.read_csv(filename, index_col=None, header=0, encoding="UTF-8", error_bad_lines=False,dtype=str, sep = ';',iterator=True, chunksize=10000)
        #df = pd.concat([chunk[(chunk[filtrolabel] == filtroValue)] for chunk in iter_csv])
        df = pd.concat([chunk for chunk in iter_csv])
        listData = []
        RNC = ''
        for index, row in df.iterrows():
          if str(row[0]).endswith('> lt all'):
            RNC = row[0].replace('> lt all','')
          if str(row[0]).startswith('UtranCell='):
            #print(row[0].split(['               ',',']))
            stringA = row[0].replace('UtranCell=','')
            stringA = stringA.replace('LocationArea=','')
            stringA = stringA.replace('ServiceArea=','')
            stringA = stringA.replace('RoutingArea=','')
            stringA = stringA.replace(' ',';')
            stringA = stringA.replace(',',';')
            stringA = stringA.split(';')
            
            stringB = RNC + ';'
            for i in stringA:
              if i != '':
                stringB += i + ';'

            #print(stringB[:-1].split(';'))
            listData.append(stringB[:-1].split(';'))
        #df = pd.DataFrame (listData, columns = ['RNC','MO','drop1','administrativeState','cId','drop2','maximumTransmissionPower','drop3','operationalState','primaryCpichPower','primaryScramblingCode','LocationArea','routingArea','drop3','drop4','uarfcnDl','uarfcnUl','Test1','Test2','Test3','Test4','Test5','Test6','Test7','Test8','Test9','Test10','Test11','Test12','Test13','Test14','Test15','Test16'])
        df = pd.DataFrame (listData, columns = ['RNC','MO','drop1','administrativeState','cId','drop2','maximumTransmissionPower','drop3','operationalState','primaryCpichPower','primaryScramblingCode','LocationArea','routingArea','drop3','drop4','uarfcnDl','uarfcnUl'])
        df = df.drop(['drop1','drop2','drop3','drop4'],1)
        li.append(df)
        
    frameSI = pd.concat(li, axis=0, ignore_index=True)
    frameSI.dropna(subset=['uarfcnUl'], how='all', inplace=True)
    frameSI = frameSI.drop_duplicates()
    frameSI['ref'] = frameSI['RNC'].astype(str) + frameSI['LocationArea'].astype(str) 
 

    fields = ['ref','LAC']
    pathImport = '/export/Dump_3G_LAC'
    getLac = ImportDF.ImportDF(fields, pathImport)
    getLac.rename(columns={'ref':'ref2'},inplace=True)

    frameSI = pd.merge(frameSI,getLac, how='left',left_on=['ref'],right_on=['ref2'])
    frameSI = frameSI.drop(['ref2','ref'], axis=1)
    frameSI.loc[frameSI['LAC'].isna(),['LAC']] = frameSI['LocationArea']



    frameSI.to_csv(csv_path,index=False,header=True,sep=';')

