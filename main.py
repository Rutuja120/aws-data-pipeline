import boto3
import pandas as pd
import pymysql
import os
s3_client = boto3.client(
    's3',
    aws_access_key_id='AKIA3LET5ZPS7NWJTYZQ',
    aws_secret_access_key='3t7hluDFeqXFjWbXoHA4p2WxTM3LrZuRKLrQ1UgX',
    region_name='us-east-1'
)

# AWS S3 Configuration
s3_client = boto3.client('s3') #hhhhh

BUCKET_NAME = 'your-s3-bucket-name'
FILE_NAME = 'data.csv'

# Download CSV file from S3
s3_client.download_file(BUCKET_NAME, FILE_NAME, FILE_NAME)

# Read CSV file using pandas
df = pd.read_csv(FILE_NAME)
print(df.head())  # Print first 5 rows


# RDS Configuration
DB_HOST = "your-rds-endpoint"
DB_USER = "your-username"
DB_PASSWORD = "your-password"
DB_NAME = "your-database"


def push_to_rds():
    """ Inserts data into an RDS database """
    try:
        connection = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor
        )
        cursor = connection.cursor()

        # Insert data into RDS
        for _, row in df.iterrows():
            sql = "INSERT INTO your_table (column1, column2) VALUES (%s, %s)"
            cursor.execute(sql, (row['column1'], row['column2']))

        connection.commit()
        print("✅ Data inserted into RDS successfully!")

    except Exception as e:
        print("❌ Error inserting into RDS:", e)
        raise  # Raising the error to trigger Glue push

    finally:
        cursor.close()
        connection.close()


def push_to_glue():
    """ Uploads CSV file to S3 for AWS Glue processing """
    glue_s3_path = f"s3://{BUCKET_NAME}/glue-data/{FILE_NAME}"
    
    try:
        s3_client.upload_file(FILE_NAME, BUCKET_NAME, f"glue-data/{FILE_NAME}")
        print(f"✅ Data uploaded to {glue_s3_path} for AWS Glue processing!")

    except Exception as e:
        print("❌ Error uploading to AWS Glue S3:", e)


if __name__ == "__main__":
    try:
        push_to_rds()
    except:
        push_to_glue()
