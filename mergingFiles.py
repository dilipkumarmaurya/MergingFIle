import json
import argparse
import boto3
import os
import threading
from fnmatch import fnmatch
print("Loading the function")
def lambda_handler(event, context):
    print("Loading the function")
    MIN_S3_SIZE = 6000000
    LOG_LEVEL = 'INFO'
    
   

    def concat(bucket, key, result_key, pattern):
        s3_client = boto3.session.Session().client('s3')
        objects_list = [x for x in list_all_objects(
            s3_client, bucket, result_key, key) if fnmatch(x[0], pattern)]
        print(
            f"Found {len(objects_list)} parts to concatenate in s3://{bucket}/{key}")
        for object in objects_list:
            print(f"Found: {object[0]} - {round(object[1]/1000, 2)}k")

        run_concatenation(s3_client, bucket, key, result_key, objects_list)


    def list_all_objects(s3_client, bucket, result_key, key):
        def format_return(resp):
            return [(x['Key'], x['Size']) for x in resp['Contents']]

        objects = []
        resp = s3_client.list_objects(Bucket=bucket, Prefix=key)
        objects.extend(format_return(resp))
        while resp['IsTruncated']:
       
            last_key = objects[-1][0]
            resp = s3_client.list_objects(
                Bucket=bucket, Prefix=key, Marker=last_key)
            objects.extend(format_return(resp))

        return objects


    def run_concatenation(s3_client, bucket, key, result_key, objects_list):
        if len(objects_list) > 1:
            upload_id = s3_client.create_multipart_upload(
                Bucket=bucket, Key=result_key)['UploadId']
            parts_mapping = assemble_parts_to_concatenate(
                s3_client, bucket, key, result_key, upload_id, objects_list)
            if len(parts_mapping) == 0:
                resp = s3_client.abort_multipart_upload(
                    Bucket=bucket, Key=result_key, UploadId=upload_id)
                print(
                    f"Aborted concatenation for file {result_filename}, parts list empty!")
            else:
                resp = s3_client.complete_multipart_upload(
                    Bucket=bucket, Key=result_key, UploadId=upload_id, MultipartUpload={'Parts': parts_mapping})
                print(
                    f"Finished concatenation for file {result_key} response was: {resp}")
        elif len(objects_list) == 1:
       
            resp = s3_client.copy_object(
                Bucket=bucket, CopySource=f"{bucket}/{objects_list[0][0]}", Key=result_key)
            print(f"Copied single file to {result_key} response was: {resp}")
        else:
            print(f"No files to concatenate for {result_filepath}")


    def assemble_parts_to_concatenate(s3_client, bucket, key, result_key, upload_id, objects_list):
        parts_mapping = []
        part_num = 0

        s3_objects = ["{}/{}".format(bucket, p[0])
                      for p in objects_list if p[1] > MIN_S3_SIZE]
        local_objects = [p[0] for p in objects_list if p[1]
                         <= MIN_S3_SIZE and not p[0] == f"{key}/"]
        total = len(s3_objects) + len(local_objects)
   
        for part_num, source_object in enumerate(s3_objects, 1):
            resp = s3_client.upload_part_copy(Bucket=bucket,
                                              Key=result_key,
                                              PartNumber=part_num,
                                              UploadId=upload_id,
                                              CopySource=source_object)
            print(f"@@@ Uploaded S3 object #{part_num} of {total}")
            parts_mapping.append(
                {'ETag': resp['CopyPartResult']['ETag'][1:-1], 'PartNumber': part_num})

   
        small_objects = []
        for source_object in local_objects:
        # Remove forward slash
            temp_filename = "/tmp/{}".format(source_object.replace("/", "_"))
            s3_client.download_file(
                Bucket=bucket, Key=source_object, Filename=temp_filename)
            with open(temp_filename, 'rb') as f:
                small_objects.append(f.read())
            os.remove(temp_filename)
            print(f"@@@ Downloaded S3 Object: {source_object}")

        if len(small_objects) > 0:
            last_part_num = part_num + 1
            last_object = b''.join(small_objects)
            resp = s3_client.upload_part(
                Bucket=bucket, Key=result_key, PartNumber=last_part_num, UploadId=upload_id, Body=last_object)
            print(f"@@@ Uploaded S3 object #{last_part_num} of {total}")
            parts_mapping.append(
                {'ETag': resp['ETag'][1:-1], 'PartNumber': last_part_num})

        return parts_mapping
    concat("hhns", "/hhns/segment-logs/weEBKBwxJF/1566086400000/","/hhns/combined.gz", "*.gz")      
  
if __name__ == "__main__":
    print("gjhkjlk;")
    parser = argparse.ArgumentParser(description="S3 Concatenation Utility.")
    parser.add_argument("--bucket", help="S3 Bucket.")
    parser.add_argument(
     "--key", help="Key/Folder Whose Contents Should Be Combined.")
    parser.add_argument(
        "--result_key", help="Output of Concatenation, Relative To The Specified Bucket.")
    parser.add_argument("--pattern", default='*',
                    help="Pattern To Match The File Names Against For Adding To The Combination.")
    args = parser.parse_args()

    print("Combining files in s3://{}/{} to s3://{}/{} matching pattern {}".format(
        "hhns", "/hhns/segment-logs/weEBKBwxJF/1566086400000/", "hhns","/hhns/combined.gz", "*.gz"))

    concat("hhns", "/hhns/segment-logs/weEBKBwxJF/1566086400000/","/hhns/combined.gz", "*.gz")
