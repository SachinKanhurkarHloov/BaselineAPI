from flask import Flask, request, jsonify
import pandas as pd
import urllib.request
import json
import os
import ssl
import sys    
import numpy as np 
import datetime


import ast   
sys.path.append('Libraries')  

import input_DB 
import  output_DB
import  baseline_kpi

app = Flask(__name__)

@app.route('/predict', methods=['POST'])

def predict():
    tag_name =  request.get_json(force=True) 

    projectid = tag_name['projectid']
    buildingid = tag_name['buildingid']
    storeyid =  tag_name['storeyid']
    target ='powerconsumptionsensor'
    tag_save = tag_name['DB_saving']

    df = input_DB.selection_api_id(projectid, buildingid, storeyid )
    ltid = df['ltid'][0] 
    df = df.set_index('time')
    # fill NaN values with the mean of each column
    df = df.fillna(df.mean())
    n = min(df.shape)

    X =  df.drop([target, 'ltid'], axis=1).values

    y =  df[target].values

    query1= X.tolist()

    def allowSelfSignedHttps(allowed):
        # bypass the server certificate verification on client side
        if allowed and not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None):
            ssl._create_default_https_context = ssl._create_unverified_context

    allowSelfSignedHttps(True) # this line is needed if you use self-signed certificate in your scoring service.

    #print(type(query1))

    data =  {
    "input_data": query1
    }

    body = str.encode(json.dumps(data))

    url = 'https://tagwaye-ml-zqldp.eastus.inference.ml.azure.com/score'
    # Replace this with the primary/secondary key or AMLToken for the endpoint
    api_key = 'mwf4go3CyCJil7cHMebLFdNXt6g0t42E'
    if not api_key:
        raise Exception("A key should be provided to invoke the endpoint")

    # The azureml-model-deployment header will force the request to go to a specific deployment.
    # Remove this header to have the request observe the endpoint traffic rules
    headers = {'Content-Type':'application/json', 'Authorization':('Bearer '+ api_key), 'azureml-model-deployment': 'baselineservicefloor1' }

    req = urllib.request.Request(url, body, headers)

    response = urllib.request.urlopen(req)

    result = response.read()

    #y_predict = float(result)
    y_predict = result.decode()
   
    y_predict = ast.literal_eval(y_predict)

    df_out = pd.DataFrame({'time': df.index, 'Baseline': y_predict, 'ltid': ltid})

    df_out['time'] = df_out['time'].astype(str)

    # Return the prediction as a JSON response
    json_data = df_out.to_json(orient='records')
    json_data = ast.literal_eval(json_data)
    output=json_data
    
    # Directly from the dictionary
    # with open('json_data.json', 'w') as outfile:
    #     json.dump(output, outfile)


    if tag_save == 'Yes':
        BaseE, ana, no_eff, no_ineff, actual_feature, predict_feature, per_eff,per_ineff,cost_eff, cost_ineff,sum_feature_eff, sum_feature_ineff= baseline_kpi.base_feature_calculation(df, y, y_predict)
        #print(BaseE,ana,no_eff,no_ineff)
        dat = baseline_kpi.data_formatting (BaseE,'Anomaly_PowerConsumption','Anomaly', ltid,'ANN')
        output_DB.output_data_upload(dat, MY_TABLE= 'baselineresults')

        dat = baseline_kpi.data_formatting (BaseE,'Actual_PowerConsumption','actuals' , ltid, 'ANN')
        output_DB.output_data_upload(dat, MY_TABLE= 'baselineresults')

        dat = baseline_kpi.data_formatting (BaseE,'Predicted_PowerConsumption','predicted', ltid, 'ANN')
        output_DB.output_data_upload(dat, MY_TABLE= 'baselineresults')

        dat =baseline_kpi.data_formatting_direct (BaseE,'Number_Eff_PowerConsumption',no_eff, ltid, 'ANN')
        output_DB.output_data_upload(dat, MY_TABLE= 'baselineresults')

        dat =baseline_kpi.data_formatting_direct (BaseE,'Number_InEff_PowerConsumption',no_ineff, ltid, 'ANN')
        output_DB.output_data_upload(dat, MY_TABLE= 'baselineresults')

        dat =baseline_kpi.data_formatting_direct (BaseE,'sum_eff_PowerConsumption',sum_feature_eff, ltid, 'ANN')
        output_DB.output_data_upload(dat, MY_TABLE= 'baselineresults')

        dat =baseline_kpi.data_formatting_direct (BaseE,'sum_ineff_PowerConsumption',sum_feature_ineff, ltid, 'ANN')
        output_DB.output_data_upload(dat, MY_TABLE= 'baselineresults')

        dat =baseline_kpi.data_formatting_direct (BaseE,'per_eff_PowerConsumption',per_eff, ltid, 'ANN')
        output_DB.output_data_upload(dat, MY_TABLE= 'baselineresults')

        dat =baseline_kpi.data_formatting_direct (BaseE,'per_ineff_PowerConsumption',per_ineff, ltid, 'ANN')
        output_DB.output_data_upload(dat, MY_TABLE= 'baselineresults')

        dat =baseline_kpi.data_formatting_direct (BaseE,'cost_eff_PowerConsumption',cost_eff, ltid,'ANN')
        output_DB.output_data_upload(dat, MY_TABLE= 'baselineresults')

        dat =baseline_kpi.data_formatting_direct (BaseE,'cost_ineff_PowerConsumption',cost_ineff, ltid, 'ANN')
        output_DB.output_data_upload(dat, MY_TABLE= 'baselineresults')
 
    return jsonify(output)

if __name__ == '__main__':
    app.run(port=5000, debug=True)

