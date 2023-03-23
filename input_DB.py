import psycopg2
import pandas as pd
from functools import reduce

conn = psycopg2.connect(database="Tagwaye-DB", user='tagwayeadmin', password='Hloov@2023', host='tagwaye2023.postgres.database.azure.com', port= '5432')



def projectid_value():
    projectid = input('Enter projectid : ')
    print('You entered : ', projectid)
    return(projectid)

def input_data_sensor():  
    projectID = projectid_value()
    ltid = input("Enter the ltid of sensor for analysis:")
    data = pd.read_sql('''select projectsensorsdata.time,projectsensorsdata.telemetry, projectsensorsdata.ltid, projectassets."name" from projectsensorsdata JOIN projectassets 
ON projectassets.id = projectsensorsdata.ltid where ltid = '{}' and projectassets.projectid = '{}' '''.format(ltid,projectID), conn)
    data = data.reset_index()
    return ltid, data

def input_data_parent():   
    projectID = projectid_value()
    parentid_list = pd.read_sql("select * from projectassets where projectassets.projectid = '{}' ".format(projectID), conn)  
    #print(parentid_list[parentid_list['parentid'], parentid_list['name']].unique())  
    print( parentid_list['parentid'].unique())
    #print(parentid_list.drop_duplicates(subset = "parentid"))
    parentid = input("Enter the parentid from above list:")

    name_list = pd.read_sql("select * from projectassets where parentid = '{}'".format(parentid), conn)
    print(name_list['name'].unique())
    n  = int(input('How many features you want to select for analysis? Select from (1, 2, 3, 5, 10):'))
    
    def df_frame(FeatureName):    
        df= pd.read_sql("""select projectsensorsdata.time,projectsensorsdata.telemetry, projectsensorsdata.ltid, projectassets."name",projectassets."name" from projectsensorsdata JOIN projectassets 
    ON projectassets.id = projectsensorsdata.ltid where projectassets.projectid = '{}' and projectassets."name" In ('{}') and  projectassets.parentid = '{}'  """.format(projectID, FeatureName, parentid ),conn)
        df.rename(columns={"telemetry": FeatureName}, inplace=True)
        df= df.drop(['name','ltid'], axis=1)
        return df

    if n==1:
        FeatureName1 = input("Enter the feature name  from above list: ")
        def combined_df_1(FeatureName1 ):
            df1 = df_frame(FeatureName=FeatureName1)
            data_frames = [df1]
            df_merged = reduce(lambda  left,right: pd.merge(left,right,on=['time'],how='outer'), data_frames)
            df_merged	= df_merged.fillna(df_merged.median())
            return df_merged
        df_m=combined_df_1(FeatureName1)
        df_m['ltid'] = parentid

    if n==2:
        FeatureName1 = input("Enter the feature name  from above list: ")
        FeatureName2 = input("Enter the feature name  from above list: ")
        def combined_df_2(FeatureName1,FeatureName2 ):
            df1 = df_frame(FeatureName=FeatureName1)
            df2 = df_frame(FeatureName=FeatureName2)
            data_frames = [df1, df2]
            df_merged = reduce(lambda  left,right: pd.merge(left,right,on=['time'],how='outer'), data_frames)
            df_merged	= df_merged.fillna(df_merged.median())
            return df_merged
        df_m=combined_df_2(FeatureName1, FeatureName2 )
        df_m['ltid'] = parentid

    elif n==3:
        FeatureName1 = input("Enter the feature name 1 from above list: ")
        FeatureName2 = input("Enter the feature name 2 from above list: ")
        FeatureName3 = input("Enter the feature name 3 from above list: ")
        def combined_df_3(FeatureName1, FeatureName2, FeatureName3 ):
            df1 = df_frame(FeatureName=FeatureName1)
            df2 = df_frame(FeatureName=FeatureName2)
            df3 = df_frame(FeatureName=FeatureName3)
            data_frames = [df1, df2, df3]
            df_merged = reduce(lambda  left,right: pd.merge(left,right,on=['time'],how='outer'), data_frames)
            df_merged	= df_merged.fillna(df_merged.median())
            return df_merged
        df_m=combined_df_3(FeatureName1, FeatureName2, FeatureName3 )
        df_m['ltid'] = parentid

    elif n==5:  
        FeatureName1 = input("Enter the feature name 1 from above list: ")
        FeatureName2 = input("Enter the feature name 2 from above list: ")
        FeatureName3 = input("Enter the feature name 3 from above list: ")
        FeatureName4 = input("Enter the feature name 4 from above list: ")
        FeatureName5 = input("Enter the feature name 5 from above list: ")
        
        def combined_df_5(FeatureName1, FeatureName2, FeatureName3, FeatureName4, FeatureName5 ):
            df1 = df_frame(FeatureName=FeatureName1)
            df2 = df_frame(FeatureName=FeatureName2)
            df3 = df_frame(FeatureName=FeatureName3)
            df4 = df_frame(FeatureName=FeatureName4)
            df5 = df_frame(FeatureName=FeatureName5)
            data_frames = [df1, df2, df3, df4, df5]
            df_merged = reduce(lambda  left,right: pd.merge(left,right,on=['time'],how='outer'), data_frames)
            df_merged	= df_merged.fillna(df_merged.median())
            return df_merged
        df_m=combined_df_5(FeatureName1, FeatureName2, FeatureName3, FeatureName4, FeatureName5 )
        df_m['ltid'] = parentid

    elif n==10:
        FeatureName1 = input("Enter the feature name 1 from above list: ")
        FeatureName2 = input("Enter the feature name 2 from above list: ")
        FeatureName3 = input("Enter the feature name 3 from above list: ")
        FeatureName4 = input("Enter the feature name 4 from above list: ")
        FeatureName5 = input("Enter the feature name 5 from above list: ")
        FeatureName6 = input("Enter the feature name 6 from above list: ")
        FeatureName7 = input("Enter the feature name 7 from above list: ")
        FeatureName8 = input("Enter the feature name 8 from above list: ")
        FeatureName9 = input("Enter the feature name 9 from above list: ")
        FeatureName10 = input("Enter the feature name 10  from above list: ")

        def combined_df_10(FeatureName1, FeatureName2, FeatureName3, FeatureName4, FeatureName5,FeatureName6, FeatureName7, FeatureName8, FeatureName9, FeatureName10  ):
            df1 = df_frame(FeatureName=FeatureName1)
            df2 = df_frame(FeatureName=FeatureName2)
            df3 = df_frame(FeatureName=FeatureName3)
            df4 = df_frame(FeatureName=FeatureName4)
            df5 = df_frame(FeatureName=FeatureName5)
            df6 = df_frame(FeatureName=FeatureName6)
            df7 = df_frame(FeatureName=FeatureName7)
            df8 = df_frame(FeatureName=FeatureName8)
            df9 = df_frame(FeatureName=FeatureName9)
            df10 = df_frame(FeatureName=FeatureName10)
            data_frames = [df1, df2, df3, df4, df5, df6, df7, df8, df9, df10]
            df_merged = reduce(lambda  left,right: pd.merge(left,right,on=['time'],how='outer'), data_frames)
            df_merged	= df_merged.fillna(df_merged.median())
            return df_merged
        df_m= combined_df_10(FeatureName1, FeatureName2, FeatureName3, FeatureName4, FeatureName5,FeatureName6, FeatureName7, FeatureName8, FeatureName9, FeatureName10  )
        df_m['ltid'] = parentid
    return parentid, df_m


def multicalldataframe(parentid,targetsensor,sensor1,sensor2,sensor3,sensor4,targetid): ## parentid  = '{}' and entitytype = 'IfcSensor'
    df = pd.read_sql(''' select projectsensorsdata."time", 
sum(case when projectassets."name" = '{}'then projectsensorsdata.telemetry end) as {},
sum(case when projectassets."name" = '{}'then projectsensorsdata.telemetry end) as {},
sum(case when projectassets."name" = '{}'then projectsensorsdata.telemetry end) as {},
sum(case when projectassets."name" = '{}'then projectsensorsdata.telemetry end) as {},
sum(case when projectassets."name" = '{}'then projectsensorsdata.telemetry end) as {},
'{}' as ltid from projectsensorsdata
JOIN projectassets ON projectassets.id = projectsensorsdata.ltid
where projectassets.parentid = '{}' and projectassets."name" in ('{}','{}','{}','{}','{}')
    group by projectsensorsdata."time"
    ORDER by projectsensorsdata."time"'''.format(targetsensor,targetsensor,sensor1,sensor1,sensor2,sensor2,sensor3,sensor3,sensor4,sensor4,targetid,parentid, targetsensor,sensor1,sensor2,sensor3,sensor4),conn)

    return df

def calldataframe(parentid,sensorname): ## parentid  = '{}' and entitytype = 'IfcSensor'
    df = pd.read_sql(''' select projectsensorsdata."time", 
sum(case when projectassets."name" = '{}'then projectsensorsdata.telemetry end) as {}, 
projectassets.id as ltid from projectsensorsdata
JOIN projectassets ON projectassets.id = projectsensorsdata.ltid
where projectassets.parentid = '{}' and projectassets."name" = '{}'
    group by projectsensorsdata."time",projectassets.id
    ORDER by projectsensorsdata."time"'''.format(sensorname,sensorname, parentid, sensorname),conn)
    print(sensorname, parentid)

    return df


def sensortelemetry(selection): # function to get sensor list
    genid = pd.read_sql("SELECT id from projectassets  where parentid = '{}' ".format(selection), conn)
    a = genid
    while len(genid)>0:
        b = tuple(list(genid['id']))
        genid = pd.read_sql("SELECT id from projectassets  where parentid in {} ".format(b), conn)
        a = a.append(genid, ignore_index=True)
        #print(len(genid))  
        sensor_list = pd.read_sql(''' SELECT sensormetadata.id,sensormetadata.ltid,sensormetadata.sensortypeid,sensortypes."name" as sensorname
                    from sensormetadata join sensortypes on sensormetadata.sensortypeid = sensortypes.id
                    where sensormetadata.ltid in {}'''.format(tuple(list(a['id']))),conn)
    print(sensor_list['sensorname'].unique()) # sensor type available in the building.
    sensorselection = input('Select your sensor type: ')
    print('calling the dataframe-creating function for this :')
    df = pd.read_sql(''' select sensormetadata.id,sensortelemetry.readingtime as time,sensortelemetry.readingvalue as {} from public.sensormetadata
    inner join sensortelemetry on sensortelemetry.ltid = sensormetadata.id
    inner join sensortypes on sensormetadata.sensortypeid = sensortypes.id
    where sensortypes.name = '{}' and sensormetadata.ltid in {} '''.format(sensorselection,sensorselection,tuple(list(sensor_list['ltid'].unique()))),conn)
    return df



def selection_api_id(projectid,buildingid,storeyid):
    
    project_list = pd.read_sql("select distinct id,name,projectid from projectassets where entitytype = 'IfcProject' ", conn) 
    print(project_list['name'].values)

  
    selection = projectid 
    
    # "Select your service 1. KPI Calculation 2. Forecast 3. Anomaly 4. Baseline 5. Classification "
    
    
    building_list = pd.read_sql("select distinct id,name,entitytype,projectid from projectassets where parentid = (select distinct id from projectassets where parentid = '{}') ".format(selection), conn)
    print(building_list['name'].values)
    
    selection = buildingid
    
    cus_choise = 'no'#input('Do you want perform the service at building level? yes or no: ') # "-- Do you want perform the service at building or you want to next level(Floor Level)?"
   
    if cus_choise == 'yes' : 
        print('Building ltid : ', selection)
        
        choise ='multiple' #input('Do you want to for forecast for single sensor or multiple: ')
     
        
        if choise == 'single':
            #sensorname = input('Select Sensor name for sensors forecast model from above list: ')
            #print('You selected sensorname = {} and its parent-id = {}'.format(sensorname, selection))
            #print('calling the dataframe-creating function for this :')
            df = sensortelemetry(selection)

        else: 
            sensor_list = pd.read_sql('''select distinct id, name as sensorname ,entitytype,parentid,projectid from projectassets where parentid = '{}' and entitytype = 'IfcSensor' '''.format(selection), conn)
            print(sensor_list['sensorname'].values)
            targetsensor = input('Select your target feature:')
            sensor1 = input('Select your Sensor Name 1 :')
            sensor2 = input('Select your Sensor Name 2 :')
            sensor3 = input('Select your Sensor Name 3 :')
            sensor4 = input('Select your Sensor Name 4 :')
            targetid = sensor_list[sensor_list['sensorname']==targetsensor]['id'].values[0]
            df = multicalldataframe(selection,targetsensor,sensor1,sensor2,sensor3,sensor4,targetid)

        
    elif cus_choise == 'no' :
        print('Building ltid : ', selection)
        #"if building level then sensor list " -- query for the sensor type (for forecast or Anamoly) -- for KPI/Baseline predefined
        #"if no than storey_list"
    
        # "-- Do you want perform the service at Floor Level or you want to next level(Space Level)?"
        storey_list = pd.read_sql("select distinct id,name,entitytype,projectid from projectassets where parentid = '{}' ".format(selection), conn)  
        print(storey_list['name'].values)
         #input('Select from the listed storey bleow : ')
        selection = storeyid
        
        cus_choise = "yes" #input('Do you want perform the service at floor level?  yes or no: ') # "-- Do you want perform the service at building or you want to next level(Floor Level)?"
        
        if cus_choise == 'yes':
            print('Floor ltid : ', selection)
            #df = sensortelemetry(selection)
            
            choise ='multiple'# input('Do you want to for forecast for single sensor or multiple: ')

            
            if choise == 'single':
#                 sensorname = input('Select Sensor name for sensors forecast model from above list: ')
#                 print('You selected sensorname = {} and its parent-id = {}'.format(sensorname, selection))
#                 print('calling the dataframe-creating function for this :')
                df = sensortelemetry(selection)

            else: 
                sensor_list = pd.read_sql('''select distinct id, name as sensorname ,entitytype,parentid,projectid from projectassets where parentid = '{}' and entitytype = 'IfcSensor' '''.format(selection), conn)
                print(sensor_list['sensorname'].values)
                targetsensor = 'PowerConsumptionSensor' # input('Select your target feature:')
                sensor1 ='HumiditySensor' #input('Select your Sensor Name 1 :')
                sensor2 ='AirVelocitySensor' #input('Select your Sensor Name 2 :')
                sensor3 ='TemperatureSensor' #input('Select your Sensor Name 3 :')
                sensor4 ='SteamEnergySensor' #input('Select your Sensor Name 4 :')
                targetid = sensor_list[sensor_list['sensorname']==targetsensor]['id'].values[0]
                df = multicalldataframe(selection,targetsensor,sensor1,sensor2,sensor3,sensor4,targetid)
                
        elif cus_choise == 'no' :
            print('Floor ltid : ', selection)
            
            
            space_list = pd.read_sql('''select distinct id, name as spacename ,entitytype,parentid,projectid from projectassets where parentid = '{}' and entitytype = 'IfcSpace' '''.format(selection), conn)
            #space_list = pd.read_sql("select distinct id,name,entitytype,projectid from projectassets where parentid = '{}' and entitytype = 'IfcSpace' ".format(selection), conn) 
            print(space_list['spacename'].values)
            space = input('Select  from the listed space above : ')
            selection = space_list[space_list['spacename']==space]['id'].values[0]
        
            cus_choise = input('Do you want perform the service at space level? yes or no: ') # "-- Do you want perform the service at building or you want to next level(Floor Level)?"

            if cus_choise == 'yes':
                #print('Space ltid : ', selection)
                #df = sensortelemetry(selection)
                
                choise = 'multiple'#input('Do you want to for forecast for single sensor or multiple: ')
                
                
                if choise == 'single':
#                     sensorname = input('Select Sensor name for sensors forecast model from above list: ')
#                     print('You selected sensorname = {} and its parent-id = {}'.format(sensorname, selection))
#                     print('calling the dataframe-creating function for this :')
                    df = sensortelemetry(selection)
                
                else:
                    sensor_list = pd.read_sql('''select distinct id, name as sensorname ,entitytype,parentid,projectid from projectassets where parentid = '{}' and entitytype = 'IfcSensor' '''.format(selection), conn)
                    print(sensor_list['sensorname'].values)
                    targetsensor = input('Select your target feature:')
                    sensor1 = input('Select your Sensor Name 1 :')
                    sensor2 = input('Select your Sensor Name 2 :')
                    sensor3 = input('Select your Sensor Name 3 :')
                    sensor4 = input('Select your Sensor Name 4 :')
                    targetid = sensor_list[sensor_list['sensorname']==targetsensor]['id'].values[0]
                    df = multicalldataframe(selection,targetsensor,sensor1,sensor2,sensor3,sensor4,targetid)

            elif cus_choise == 'no' :
                    print('space ltid : ', selection)
            else:
                print('Not a vaild entry')
                
        else:
                print('Not a vaild entry')
    else:
        print('Not a vaild entry')

    #     df = calldataframe(selection,sensorname)
    return df
