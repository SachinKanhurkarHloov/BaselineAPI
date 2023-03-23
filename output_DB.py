import psycopg2
import pandas as pd
import datetime

def output_data(dat,ltid ):
    dat.to_csv('Forecast_Feature_{}.csv'.format(ltid), index= False)

#############

import psycopg2
import datetime as dt
import pandas as pd

def output_data_upload(dat, MY_TABLE):
    #host = "tagwaye2023.postgres.database.azure.com"
    #dbname = "Tagwaye-DB"
    #user = "tagwayeadmin"
    #password = "Hloov@2023"
    #sslmode = "require"

    try:
        #conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)
        #conn = psycopg2.connect(conn_string)
        conn = psycopg2.connect(
    database="Tagwaye-DB", user='tagwayeadmin', password='Hloov@2023', host='tagwaye2023.postgres.database.azure.com', port= '5432')
        print("Connection established")
        # get a cursor object from the connection
        cur = conn.cursor()

        # prepare data insertion rows
        dataInsertionTuples =  dat.values.tolist()

        # create sql command for rows insertion
        dataText = ','.join(cur.mogrify('(%s,%s,%s,%s,%s,%s)', row).decode(
            "utf-8") for row in dataInsertionTuples)

        sqlTxt = '''INSERT INTO {}( time,  kpiname, kpivalue, ltid, modelname, timeofupload) values{}'''.format(MY_TABLE,dataText)
        
        # execute the sql to perform insertion
        cur.execute(sqlTxt)

        rowCount = cur.rowcount
        print("number of inserted rows =", rowCount)

        # commit the changes
        conn.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while interacting with PostgreSQL...\n", error)
    finally:
        if(conn):
            # close the cursor object to avoid memory leaks
            cur.close()
            # close the connection object also
            conn.close()

    print("data insertion example code execution complete...")

def output_data_upload_forecast(dat, MY_TABLE):
    #host = "tagwaye2023.postgres.database.azure.com"
    #dbname = "Tagwaye-DB"
    #user = "tagwayeadmin"
    #password = "Hloov@2023"
    #sslmode = "require"

    try:
        #conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)
        #conn = psycopg2.connect(conn_string)
        conn = psycopg2.connect(
    database="Tagwaye-DB", user='tagwayeadmin', password='Hloov@2023', host='tagwaye2023.postgres.database.azure.com', port= '5432')
        print("Connection established")
        # get a cursor object from the connection
        cur = conn.cursor()

        # prepare data insertion rows
        dataInsertionTuples =  dat.values.tolist()

        # create sql command for rows insertion
        dataText = ','.join(cur.mogrify('(%s,%s,%s,%s,%s,%s)', row).decode(
            "utf-8") for row in dataInsertionTuples)

        sqlTxt = '''INSERT INTO {} ( time,  kpiname, kpiforecast, ltid, modelname, timeofupload) values{}'''.format(MY_TABLE,dataText)

        # execute the sql to perform insertion
        cur.execute(sqlTxt)

        rowCount = cur.rowcount
        print("number of inserted rows =", rowCount)

        # commit the changes
        conn.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while interacting with PostgreSQL...\n", error)
    finally:
        if(conn):
            # close the cursor object to avoid memory leaks
            cur.close()
            # close the connection object also
            conn.close()

    print("data insertion example code execution complete...")

def data_formatting (ana,kpiname,kpivaluename,ltid, modelname):
    UploadTime = datetime.datetime.now()
    #kpiname = input('Enter the KPI name for save data')
    dat = pd.DataFrame()
    dat['time'] = ana['time']
    dat['kpiname'] = kpiname
    dat['kpivalue'] =ana[kpivaluename]
    #dat['kpiforecating'] = np.nan
    dat['ltid'] = ltid
    dat['modelname'] =modelname
    dat['timeofupload'] = UploadTime    
    return dat