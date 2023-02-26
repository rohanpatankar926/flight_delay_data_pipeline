import os

class S3Sync:
    def s3_csv_folder_sync(self,folder,aws_bucket_uri):
        command=f"aws s3 sync {folder} {aws_bucket_uri}"
        os.system(command)

    def sync_folder_from_s3(self,folder,aws_bucket_uri):
        command=f"aws s3 sync {aws_bucket_uri} {folder}"
        os.system(command)