import io
from minio import Minio
from minio.error import S3Error
from config.config import config
import pandas as pd
from datetime import timedelta
import logging

def main():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

    try:
        # Initialize MinIO client
        client = Minio(
            'localhost:9090',
            access_key=config["access_key"],
            secret_key=config["secret_key"],
            secure=False  # Set to True if using SSL
        )
        logger.info("Connected to MinIO successfully.")

        bucket_name = 'bronze'
        target_bucket = 'nyc-taxis-records'
        target_prefix = 'nyc_taxi_record/'

        # Check if the source bucket exists
        if not client.bucket_exists(bucket_name):
            logger.error(f"Source bucket '{bucket_name}' does not exist.")
            return

        # Check if the target bucket exists; create if it doesn't
        if not client.bucket_exists(target_bucket):
            client.make_bucket(target_bucket)
            logger.info(f"Target bucket '{target_bucket}' created.")
        else:
            logger.info(f"Target bucket '{target_bucket}' already exists.")

        # List objects in the source bucket
        objects = client.list_objects(bucket_name, recursive=True)

        processed_objects = 0  # Counter for processed objects

        for obj in objects:
            if 'nyc_taxi_files' in obj.object_name and obj.object_name.endswith('.parquet'):
                logger.info(f"Processing object: {obj.object_name}")

                try:
                    # Generate a presigned URL for the object
                    url = client.get_presigned_url(
                        'GET',
                        bucket_name,
                        obj.object_name,
                        expires=timedelta(hours=1)
                    )
                    logger.info(f"Generated presigned URL for {obj.object_name}")

                    # Read the Parquet data from the URL
                    data = pd.read_parquet(url, engine='pyarrow')  # Use pyarrow or fastparquet
                    logger.info(f"Read {len(data)} rows from {obj.object_name}")

                    # Iterate over each row in the DataFrame
                    for index, row in data.iterrows():
                        try:
                            vendor_id = str(row['VendorID'])  # Corrected 'VendiorID' to 'VendorID'
                            pickup_datetime = str(row['tpep_pickup_datetime'])
                            pickup_datetime_formatted = pickup_datetime.replace(':', '-').replace(' ', '_')
                            file_name = f'trip_{vendor_id}_{pickup_datetime_formatted}.json'

                            # Convert row to JSON
                            record = row.to_json()
                            record_bytes = record.encode('utf-8')  
                            record_stream = io.BytesIO(record_bytes)
                            record_stream_len = len(record_bytes)  

                            # Upload the JSON record to the target bucket
                            client.put_object(
                                target_bucket,
                                f'{target_prefix}{file_name}',
                                data=record_stream,
                                length=record_stream_len,
                                content_type='application/json'
                            )

                            logger.info(f'Uploaded {file_name} to MinIO.')

                        except KeyError as ke:
                            logger.error(f"Missing expected column in row: {ke}")
                        except Exception as e:
                            logger.error(f"Error processing row {index} in {obj.object_name}: {e}")

                    processed_objects += 1

                except S3Error as e:
                    logger.error(f"S3Error while processing {obj.object_name}: {e}")
                except pd.errors.EmptyDataError:
                    logger.warning(f"No data to read in {obj.object_name}.")
                except Exception as e:
                    logger.error(f"Unexpected error while processing {obj.object_name}: {e}")

        if processed_objects == 0:
            logger.warning(f"No objects containing 'nyc_taxi_files' with '.parquet' extension found in bucket '{bucket_name}'.")

    except S3Error as e:
        logger.error(f"Error occurred while connecting to MinIO: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
