# SpringBoot-AWS-S3

upload files in AWS S3 

Swagger UI
------------------
http://localhost:8080/swagger-ui.html#/


~~~
@patch('boto3.client')
    def test_delete_existing_s3_directory(self, mock_boto_client):
        # Mock S3 client
        mock_s3_client = MagicMock()
        
        # Mock boto3 client to return the mock S3 client
        mock_boto_client.return_value = mock_s3_client
        
        # Mock response for list_objects_v2
        mock_s3_client.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'test-prefix/file1.txt'},
                {'Key': 'test-prefix/file2.txt'},
                {'Key': 'test-prefix/file3.txt'}
            ],
            'KeyCount': 3
        }
        
        # Test the function
        bucket_name = 'test-bucket'
        s3_key = 'test-prefix/'
        delete_existing_s3_directory(mock_s3_client, bucket_name, s3_key)
        
        # Assertions
        mock_s3_client.list_objects_v2.assert_called_once_with(Bucket=bucket_name, Prefix=s3_key)
        mock_s3_client.delete_objects.assert_called_once_with(
            Bucket=bucket_name,
            Delete={
                'Objects': [
                    {'Key': 'test-prefix/file1.txt'},
                    {'Key': 'test-prefix/file2.txt'},
                    {'Key': 'test-prefix/file3.txt'}
                ]
            }
        )
~~~
