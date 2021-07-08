from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound
import os
import pathlib
from datetime import datetime, timedelta
import shutil
from random import randint

def download_large_table_to_gcs(table_path, bucket_name,
                              format_suffix = ".csv"):
    '''
        table_path - Example is "maxinal-furnace-783.rohitrr.sample_test_table"
        gcs_file_name - Should be of the form file_name/*.csv in case
                        the table is expected to be larger than 1GB
    
    Output -  A folder name with the same table name (and extra suffixes) which is 
    created in the gcs bucket. The table is partitioned and stored in the folder
    '''
    
    print("Downloading table - {} to gcs".format(table_path))
    temp = table_path.split('.')
    project = temp[0]
    dataset_id = temp[1]
    table_id = gcs_folder_name = temp[2]
    
#     In order to to ensure files don't get simply added to exisitng folders unique suffixes are generated
    unique_suffix = datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S') +\
                    '_' + str(randint(0, 200))
    gcs_folder_name = gcs_folder_name + '_' + unique_suffix

    
#     As the table is large gcs file name must be a wild card for google 
#     cloud to partition and download the table
    gcs_file_name = gcs_folder_name + "/*"+ format_suffix
#     gcs_file_name will be like file_name/*.csv

    destination_uri = "gs://{}/{}".format(bucket_name, gcs_file_name)
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table_id)
    
    job_config = bigquery.ExtractJobConfig(field_delimiter=",")
    bq_client = bigquery.Client()
    extract_job = bq_client.extract_table(
        table_ref,
        destination_uri,
        job_config = job_config,
        # Location must match that of the source table.
        location="US",
    )  # API request
    extract_job.result()  # Waits for job to complete.

    print(
        "Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri)
    )
    return gcs_folder_name
    
def download_files_from_folder_in_gcs(bucket_name, gcs_folder_name, dest_folder_path):
    print("Downloading from gcs_folder_name {} to local".
          format(gcs_folder_name))
    
    pathlib.Path(dest_folder_path).mkdir(parents = True, exist_ok = True)

    storage_client = storage.Client(project="maximal-furncace-783")
    bucket = storage_client.get_bucket(bucket_or_name=bucket_name)
    blobs = bucket.list_blobs(prefix=gcs_folder_name)  # Get list of files
    for blob in blobs:
        filename = blob.name.replace('/', '_') 
        blob.download_to_filename(os.path.join(dest_folder_path,
                                              filename))

    
    print(f"Contents in gs://{bucket_name}/{gcs_folder_name} \
    transferred to {dest_folder_path}")

def merge_and_save_csv_files(csv_files_folder_path, dest_folder_path, with_common_header = True,
                             out_file_name = "merged_out.txt"):
    print(f"Merging and saving files from {csv_files_folder_path} to {dest_folder_path}")
    
    pathlib.Path(dest_folder_path).mkdir(parents = True, exist_ok = True)
    csv_file_names = [file_name for file_name \
                  in os.listdir(csv_files_folder_path)\
                 if file_name.endswith(".csv")]
    
    f_out = open(os.path.join(dest_folder_path, out_file_name), 'w')
    for csv_file_name in csv_file_names:
        csv_file_path = os.path.join(csv_files_folder_path, csv_file_name)
        with open(csv_file_path) as f_csv_in:
            header = next(f_csv_in)
            if(with_common_header):
                f_out.write(header)
                with_common_header = False # After writing header once, do not write again
                
            for line in f_csv_in:
                f_out.write(line)
    f_out.close()
    print(f"Saved file {out_file_name} in {dest_folder_path}")
    
def download_table_to_local_as_one_file(table_path, local_save_path, out_file_name = "fetched_table.csv",
                                        with_header = True, bucket_name = "query_runner_results"):
    local_download_folder_path = os.path.join(local_save_path, "temp_download_folder")
    if(os.path.exists(local_download_folder_path)):
        print(f"Temporary download folder - {local_download_folder_path} \
              already present - removing it to avoid using older files")
        shutil.rmtree(local_download_folder_path)
        print("Old temp folder removed")
        
    merged_out_path = local_save_path
    
    downloaded_gcs_folder_name = download_large_table_to_gcs(table_path, bucket_name)
    download_files_from_folder_in_gcs(bucket_name, downloaded_gcs_folder_name,
                                     local_download_folder_path)
    merge_and_save_csv_files(local_download_folder_path, merged_out_path, with_common_header=with_header,
                            out_file_name=out_file_name)
    
    shutil.rmtree(local_download_folder_path) 