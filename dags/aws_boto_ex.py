# https://docs.aws.amazon.com/boto3/latest/reference/services/s3.html
# https://docs.aws.amazon.com/boto3/latest/guide/session.html
# https://docs.aws.amazon.com/boto3/latest/reference/services/s3/client/create_bucket.html
# https://docs.aws.amazon.com/boto3/latest/reference/services/s3/client/upload_file.html

# finance api:  https://ranaroussi.github.io/yfinance/reference/api/yfinance.download.html#yfinance.download



# Note this is not a DAG, just a utility method needed for DAG to run - thats why its present in   .airflowignore file

import boto3
import boto3.session
import logging
import yfinance

logging.basicConfig(level=logging.INFO)


def create_bucket(client,desired_bukcet_name):
    # check if bucket exists
    bucket_exists= False
    
    response = client.list_buckets()

    for i in response['Buckets']:
        if(i['Name']==desired_bukcet_name):
            logging.info("Bucket already exists")
            bucket_exists=True
            break

    if(bucket_exists==False):
        logging.info("Creating bucket...")
        client.create_bucket(Bucket=desired_bukcet_name)
        logging.info(f"Bucket {desired_bukcet_name} created")


def upload_files(client,file_name,desired_bukcet_name):

    logging.info("Calling API ....")

    d=yfinance.download(tickers=['AAPL'],multi_level_index=False)
    d.to_csv('stock.csv')
    logging.info("File downloaded from API successfully ")
    
    client.upload_file(file_name,desired_bukcet_name,file_name)
    
    logging.info("File uploaded successfully")


def download_file(client,desired_bukcet_name):

    logging.info("listing files....")

    f=client.list_objects(Bucket=desired_bukcet_name,Prefix='op/')

    files_arr=f['Contents']

    for i in files_arr:

        file_format=''.join(list(reversed(i['Key'][-1:-4:-1])))

        if(file_format=='csv'):
            logging.info(f"downloading file.....{i['Key']}")

            client.download_file(desired_bukcet_name,i['Key'],"/tmp/spark_op.csv")

            logging.info(f"file downloaded    {i['Key']}")


def orchestrate(client,desired_bukcet_name,file_name):

    '''
    
    Function responsible for:

    1) creating bucket if not exists
    2) uploading file to bucket

    If any error the function returns False else returns True
    
    '''

    error_flag=True

    try:

        create_bucket(client,desired_bukcet_name)

        upload_files(client,file_name,desired_bukcet_name)
    
    except Exception as e:
        logging.error(e)
        error_flag=False 
    
    
    return error_flag

if(__name__=='__main__'):

    # constants
    desired_bukcet_name="stock-data"
    file_name= "stock.csv"

    
    my_session=boto3.session.Session(aws_access_key_id= 'changemeuser',aws_secret_access_key='changemepass')
    client= my_session.client(service_name='s3',endpoint_url='http://127.0.0.1:9000/')

    #orchestrate(client,desired_bukcet_name,file_name)
    download_file(client,desired_bukcet_name)






