import pandas as pd
import boto3
from io import BytesIO
import gc
import tempfile
import os
import math
from datetime import datetime, timedelta


# Default minimum size of file should be 5MB
def upload_parquet_to_s3_with_transaction(dataframe, bucket_name, s3_file_key, aws_access_key_id, aws_secret_access_key, min_chunk_size=5 * 1024 * 1024):
    # Create a boto3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    
    # Convert the entire DataFrame to a Parquet file in memory
    parquet_buffer = BytesIO()
    dataframe.to_parquet(parquet_buffer, index=False, compression='gzip')
    parquet_buffer.seek(0)
    
    # Check the size of the buffer
    buffer_size = parquet_buffer.getbuffer().nbytes

    if buffer_size <= min_chunk_size:
        # Single upload if the buffer size is smaller than or equal to the minimum chunk size
        s3_client.put_object(Bucket=bucket_name, Key=s3_file_key, Body=parquet_buffer)
        print(f"File uploaded to s3://{bucket_name}/{s3_file_key} using single upload")
    else:
        # Multipart upload if the buffer size is larger than the minimum chunk size
        multipart_upload = s3_client.create_multipart_upload(Bucket=bucket_name, Key=s3_file_key)
        upload_id = multipart_upload['UploadId']
        parts = []

        try:
            part_number = 1
            while parquet_buffer.tell() < buffer_size:
                chunk_data = parquet_buffer.read(min_chunk_size)
                if len(chunk_data) == 0:
                    break
                part = s3_client.upload_part(
                    Bucket=bucket_name,
                    Key=s3_file_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=chunk_data
                )
                parts.append({'PartNumber': part_number, 'ETag': part['ETag']})
                part_number += 1

            # Complete multipart upload
            s3_client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=s3_file_key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
            print(f"File uploaded to s3://{bucket_name}/{s3_file_key} using multipart upload")
        except Exception as e:
            s3_client.abort_multipart_upload(Bucket=bucket_name, Key=s3_file_key, UploadId=upload_id)
            print(f"Upload failed and aborted: {e}")

def delete_existing_s3_directory(s3_client, bucket_name, s3_key):
    # List all objects with the specified prefix
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_key)
    if 'Contents' in response:
        objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
        
        # Delete the listed objects
        s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': objects_to_delete})
        print(f"Deleted existing directory: s3://{bucket_name}/{s3_key}, count:{response['KeyCount']}")



def upload_dataframe_in_chunks(dataframe, bucket_name, s3_key,  base_file_name, aws_access_key_id, aws_secret_access_key, chunk_size=3):
    # Create a boto3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    delete_existing_s3_directory(s3_client, bucket_name, s3_key)
    # Calculate the number of chunks needed
    number_of_chunks = math.ceil(len(dataframe) / chunk_size)
    for i in range(number_of_chunks):
        start_index = i * chunk_size
        end_index = (i + 1) * chunk_size
        chunk_df = dataframe.iloc[start_index:end_index]
        print("chunk_df size :", len(chunk_df))

        # Convert the DataFrame chunk to a Parquet file in memory
        parquet_buffer = BytesIO()
        chunk_df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        # Define a unique key for each chunk
        s3_file_key = f"{s3_key}{base_file_name}_part_{i}.parquet"

        # Upload the chunk to S3
        s3_client.put_object(Bucket=bucket_name, Key=s3_file_key, Body=parquet_buffer)
        print(f"Chunk {i} uploaded to s3://{bucket_name}/{s3_file_key}")


def upload_dataframe_in_chunks_with_temp_file(dataframe, bucket_name, s3_key,  base_file_name, aws_access_key_id, aws_secret_access_key, chunk_size=3):
    # Create a boto3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    delete_existing_s3_directory(s3_client, bucket_name, s3_key)
    # Calculate the number of chunks needed
    number_of_chunks = math.ceil(len(dataframe) / chunk_size)
    for i in range(number_of_chunks):
        start_index = i * chunk_size
        end_index = (i + 1) * chunk_size
        chunk_df = dataframe.iloc[start_index:end_index]

        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            chunk_df.to_parquet(temp_file.name, index=False)
            temp_file_path = temp_file.name

        # Define a unique key for each chunk
        s3_file_key = f"{s3_key}{base_file_name}_part_{i}.parquet"
        buffer_size_kb = os.path.getsize(temp_file_path) / 1024

        # Upload the chunk to S3
        s3_client.upload_file(temp_file_path, bucket_name, s3_file_key)
    
        print(f"Chunk {i} uploaded to s3://{bucket_name}/{s3_file_key} with records: {len(chunk_df)} , size : {round(buffer_size_kb, 2)} KB")
        os.remove(temp_file_path)


def getDate(dateformat):
    return (datetime.now() - timedelta(1)).strftime(dateformat)


# Example usage
if __name__ == "__main__":
    # Sample DataFrame
    data = {
        'column1': range(11),  # Large dataset
        'column2': ['A']*11
    }
    df = pd.DataFrame(data)

    # AWS S3 configuration
    bucket_name = '<bucket_name>'
    aws_access_key_id = '<aws_access_key_id>'
    aws_secret_access_key = '<aws_secret_access_key>'
    
    bucket_dir = f'period={getDate('%Y-%m-%d')}/'

    s3_key = f'data/myfiles/{bucket_dir}'
    base_file_name = f'test-{getDate('%H%M%S')}'
    
    # Upload DataFrame to S3 as Parquet
    upload_dataframe_in_chunks(df, bucket_name, s3_key, base_file_name, aws_access_key_id, aws_secret_access_key, 2)
