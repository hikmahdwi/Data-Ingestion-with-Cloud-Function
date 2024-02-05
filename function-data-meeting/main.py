import functions_framework
from google.cloud import storage
from google.cloud import bigquery
from json import dumps, loads

@functions_framework.http
def bp_meeting(request):
    request_json = request.get_json(silent=True)
    if request_json and 'data' in request_json:
        data = request_json['data']
        datas = loads(data)
        print("espo: ", datas)

        trans = {}
        for key, val in datas.items():
            if type(val) is list:
                if val != []:
                    new_val = val[0]
                else:
                    new_val = None
                trans[key] = new_val
            elif type(val) is dict:
                if val != {}:
                    new_val = list(val.values())
                    trans[key] = new_val[0]
                else:
                    new_val = None
                    trans[key] = new_val
            else:
                trans[key] = val

        dataf_col = list(trans.keys())

        client_go = bigquery.Client()
        project = "business-process-412108"
        dataset = "bp_aptana"
        table = "bp_meeting"

        dataset_ref = client_go.dataset(dataset, project=project)
        table_ref = dataset_ref.table(table)
        table_ref = client_go.get_table(table_ref)

        result_bq = ["{0}".format(schema.name) for schema in table_ref.schema]
        print(result_bq)
 
        bq_col = set(result_bq)
        fl_col = set(dataf_col)
        diff_col = list(fl_col.difference(bq_col))

        if diff_col != []:
            ctable = client_go.get_table("business-process-412108.bp_aptana.bp_meeting")

            original_schema = ctable.schema
            new_schema = original_schema[:]
            for col in diff_col:
                new_schema.append(bigquery.SchemaField(col, "STRING"))

            ctable.schema = new_schema
            ctable = client_go.update_table(ctable, ["schema"]) 

            if len(new_schema) == len(original_schema):
                print("The column has not been added.")
            else:
                print("A new column has been added.")
        else:
            print("There is No Schema Changes")

        
        clien_storage = storage.Client(project='business-process-412108')
        bucket = clien_storage.bucket('business_process_data_lake')
        
        blob = bucket.blob('bp_meeting/meeting_'+ trans['id'] +'.json')
        with blob.open("w") as file:
            file.write(dumps(trans))
        file.close()

        print("Success Upload Data to Cloud Storage")
        return 'Success Upload Data to Cloud Storage'
    else:
        print('No Data Found')
        return 'No Data Found'
