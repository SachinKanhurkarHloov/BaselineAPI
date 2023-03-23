import pandas as pd
import numpy as np
import datetime
import json
from math import sqrt

def detect_classify_anomalies(df,window):
    df.replace([np.inf, -np.inf], np.NaN, inplace=True)
    df.fillna(0,inplace=True)
    df['error']=df['actuals']-df['predicted']
    df['percentage_change'] = ((df['actuals'] - df['predicted']) / df['actuals']) * 100
    df['meanval'] = df['error'].rolling(window=window).mean()
    df['deviation'] = df['error'].rolling(window=window).std()
    df['-3s'] = df['meanval'] - (2 * df['deviation'])
    df['3s'] = df['meanval'] + (2 * df['deviation'])
    df['-2s'] = df['meanval'] - (1.75 * df['deviation'])
    df['2s'] = df['meanval'] + (1.75 * df['deviation'])
    df['-1s'] = df['meanval'] - (1.5 * df['deviation'])
    df['1s'] = df['meanval'] + (1.5 * df['deviation'])
    cut_list = df[['error', '-3s', '-2s', '-1s', 'meanval', '1s', '2s', '3s']]
    cut_values = cut_list.values
    cut_sort = np.sort(cut_values)
    df['impact'] = [(lambda x: np.where(cut_sort == df['error'][x])[1][0])(x) for x in
                            range(len(df['error']))]
    severity = {0: 3, 1: 2, 2: 1, 3: 0, 4: 0, 5: 1, 6: 2, 7: 3}
    region = {0: "NEGATIVE", 1: "NEGATIVE", 2: "NEGATIVE", 3: "NEGATIVE", 4: "POSITIVE", 5: "POSITIVE", 6: "POSITIVE",
            7: "POSITIVE"}
    df['color'] =  df['impact'].map(severity)
    df['region'] = df['impact'].map(region)
    df['anomaly_points'] = np.where(df['color'] == 3, df['error'], np.nan)
    df = df.sort_values(by='time', ascending=True)
    df.time = pd.to_datetime(df['time'].astype(str), format="%Y-%m-%d %H:%M:%S")
    df['color'][0:168]=1
    return df

def base_feature_calculation(df, y, y_predict):
    BaseE = pd.DataFrame()
    BaseE['time'] = df.index
    BaseE['actuals']=y
    BaseE['predicted'] = y_predict
    BaseE = detect_classify_anomalies(BaseE, 24*7)
    BaseE['Anomaly']=0
    for i in range(len(BaseE)):
        if BaseE['color'][i]==3:
            BaseE['Anomaly'][i] = 1
        else:
            BaseE['Anomaly'][i] = 0
    BaseE['number_efficiency']=0
    BaseE['number_inefficiency']=0
    sum_feature_eff = 0
    sum_feature_ineff = 0
    for i in range(len(BaseE)):
        if BaseE['error'][i]<= 0:
            BaseE['number_efficiency'][i] = 1
            BaseE['number_inefficiency'][i] = 0
            sum_feature_eff = sum_feature_eff+ BaseE['error'][i]
        else:
            BaseE['number_efficiency'][i] = 0
            BaseE['number_inefficiency'][i] = 1
            sum_feature_ineff = sum_feature_ineff+ BaseE['error'][i]

    print(BaseE.head())

    ana = sum (BaseE['Anomaly'])
    print(ana)

    no_eff = sum (BaseE['number_efficiency'])
    print(no_eff)

    no_ineff = sum (BaseE['number_inefficiency'])
    print(no_ineff)

    actual_feature = sum (BaseE['actuals'])
    predict_feature = sum (BaseE['predicted'])

    per_eff= (abs(sum_feature_eff)/predict_feature)*100
    print(per_eff)

    per_ineff= (abs(sum_feature_ineff)/predict_feature)*100
    print(per_ineff)

    cost_eff= 0.100*sum_feature_eff

    cost_ineff= 0.100*sum_feature_ineff
    
    return BaseE, ana, no_eff, no_ineff, actual_feature, predict_feature, per_eff,per_ineff,cost_eff, cost_ineff,sum_feature_eff,sum_feature_ineff


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


def data_formatting_direct (ana,kpiname,kpivaluename,ltid, modelname):
    UploadTime = datetime.datetime.now()
    #kpiname = input('Enter the KPI name for save data')
    dat = pd.DataFrame()
    dat= pd.DataFrame([[ ana['time'][len(ana['time'])-1]]], columns=['time'])
    dat['time'] = ana['time']
    dat['kpiname'] = kpiname
    dat['kpivalue'] =kpivaluename
    #dat['kpiforecating'] = np.nan
    dat['ltid'] = ltid
    dat['modelname'] =modelname
    dat['timeofupload'] = UploadTime    
    return dat