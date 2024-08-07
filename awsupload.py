import pandas as pd
import boto3
from io import BytesIO
import gc
import math
from datetime import datetime

def upload_dataframe_in_chunks(dataframe, bucket_name, s3_file_key_prefix, aws_access_key_id, aws_secret_access_key, chunk_size=10):
    # Create a boto3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    # Calculate the number of chunks needed
    number_of_chunks = math.ceil(len(dataframe) / chunk_size)
    for i in range(number_of_chunks):
        start_index = i * chunk_size
        end_index = (i + 1) * chunk_size
        chunk_df = dataframe.iloc[start_index:end_index]
        print("chunk_df size :", len(chunk_df))
        # Convert the DataFrame chunk to a Parquet file in memory
        parquet_buffer = BytesIO()
        chunk_df.to_parquet(parquet_buffer, index=False, compression='gzip')
        parquet_buffer.seek(0)

        # Define a unique key for each chunk
        s3_file_key = f"{s3_file_key_prefix}_part_{i}.parquet"

        # Upload the chunk to S3
        s3_client.put_object(Bucket=bucket_name, Key=s3_file_key, Body=parquet_buffer)
        print(f"Chunk {i} uploaded to s3://{bucket_name}/{s3_file_key}")

# Example usage
if __name__ == "__main__":
    # Sample DataFrame
    data = {
        'column1': range(11),  # Large dataset
        'column2': ['A']*11
    }
    df = pd.DataFrame(data)

    # AWS S3 configuration
    bucket_name = '<bucketname>'
    s3_file_key = 'data/files' + str(datetime.now())
    aws_access_key_id = '<key_id>'
    aws_secret_access_key = '<access_key>'

    # Upload DataFrame to S3 as Parquet
    upload_dataframe_in_chunks(df, bucket_name, s3_file_key, aws_access_key_id, aws_secret_access_key)
