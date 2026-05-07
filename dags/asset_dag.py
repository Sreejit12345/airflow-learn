from airflow.sdk import asset

'''
This code defines an asset dependency, so generate_asset fucntions runs every day and updates asset "generate_asset"
"generate_location" assets gets triggered based on "generate_asset" and executes function "generate_location"


Essentially 2 dags and 2 asssets are created with internal dependencies

'''

# the uri can be any valid https schema, at the moment airflow does not take action based on uri

@asset(uri="https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json",schedule="@daily")
def generate_asset(self):    #self referes to the object of asset class here , 
                             #if we dont need params from asset decorator we can give empty signature as well
    import requests    #inporting here to imporve dag parsing speed
    resp=requests.get(self.uri)
    return resp.json()    #pushed as xcom





@asset(schedule=generate_asset)     #gets triggered as soon as generate_asset is materialized
def generate_location(context):    #here context dictionary is passed

    resp=context['ti'].xcom_pull(task_ids="generate_asset",
                            key="return_value",
                            include_prior_dates=True)

    if(resp is None):
        return None

    return resp['personalInfo']['address']



