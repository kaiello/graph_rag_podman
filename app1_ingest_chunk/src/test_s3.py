import boto3
import os
from botocore.exceptions import ClientError, NoCredentialsError

def test_s3_connection():
    bucket_name = os.getenv("S3_BUCKET_NAME")
    
    print(f"--- Starting S3 Connection Test ---")
    print(f"Target Bucket: {bucket_name}")

    if not bucket_name:
        print("Error: S3_BUCKET_NAME environment variable is missing.")
        return

    try:
        # Initialize S3 client using env vars automatically picked up by boto3
        s3 = boto3.client('s3')
        
        # Attempt to list objects (this verifies read permissions)
        response = s3.list_objects_v2(Bucket=bucket_name, MaxKeys=5)
        
        if 'Contents' in response:
            print("\nSUCCESS: Connection established! Found the following files:")
            for obj in response['Contents']:
                print(f" - {obj['Key']}")
        else:
            print("\nSUCCESS: Connection established! (Bucket is empty)")
            
    except NoCredentialsError:
        print("\nFAILURE: No AWS credentials found. Check your .env file.")
    except ClientError as e:
        print(f"\nFAILURE: AWS Error: {e}")
    except Exception as e:
        print(f"\nFAILURE: Unexpected error: {e}")
    finally:
        print("--- End Test ---")

if __name__ == "__main__":
    test_s3_connection()