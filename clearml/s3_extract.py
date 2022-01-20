import os
import zipfile, tarfile
from io import BytesIO
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.client import Config

GB = 1024 ** 3
NO_MPUPLOAD_CONFIG = TransferConfig(multipart_threshold=20 * GB)


def buffered_read(stream_body, chunksize=1 * GB):
    byte_arr = bytearray()
    for i, chunk in enumerate(stream_body.iter_chunks(chunk_size=chunksize)):
        byte_arr.extend(chunk)
        print(f"Downloaded {len(byte_arr)//GB}GB so far (chunk {i})..")
    return byte_arr


def extract_upload(
    s3_resource,
    obj,
    dest_bucket,
    upload_dir_path,
    verbose=False,
    filetype="zip",
    disable_mpupload=False,
):
    upload_dir_path = Path(upload_dir_path)
    if filetype == "zip":
        with BytesIO() as buffer:
            print(f"Reading {filetype} file..")
            obj.download_fileobj(buffer)
            print("Iterating through file")
            z = zipfile.ZipFile(buffer)
            for filename in z.namelist():
                file_info = z.getinfo(filename)
                if file_info.is_dir():
                    if verbose:
                        print(f"Skipping {filename} as it is dir.")
                    continue

                upload_path = upload_dir_path / filename
                if verbose:
                    print(filename)
                    print("Uploading to", upload_path)
                s3_resource.meta.client.upload_fileobj(
                    z.open(filename),
                    Bucket=dest_bucket,
                    Key=f"{upload_path}",
                    Config=NO_MPUPLOAD_CONFIG if disable_mpupload else None,
                )
    elif filetype == "tar" or filetype == "tar.gz":
        mode = "r:gz" if filetype == "tar.gz" else "r"
        with BytesIO(buffered_read(obj.get()["Body"])) as buffer:
            with tarfile.open(fileobj=buffer, mode=mode) as tar:
                for tarinfo in tar:
                    fname = tarinfo.name
                    if not tarinfo.isfile():
                        continue
                    if fname is None:
                        continue
                    upload_path = upload_dir_path / fname
                    if verbose:
                        print(fname)
                        print("Uploading to", upload_path)

                    s3_resource.meta.client.upload_fileobj(
                        tar.extractfile(tarinfo),
                        Bucket=dest_bucket,
                        Key=f"{upload_path}",
                        Config=NO_MPUPLOAD_CONFIG if disable_mpupload else None,
                    )

def extract(src_bucket, src_path, dst_path, dst_bucket=None, src_is_dir=False, verbose=False):
    src_buck = src_bucket
    dst_buck = dst_bucket if dst_bucket else src_buck

    upload_folder = Path(dst_path)
    upload_folder.mkdir(parents=True, exist_ok=True)

    print(f"Connecting to {os.environ.get('AWS_ENDPOINT_URL')}")
    s3_resource = boto3.resource(
        "s3",
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        verify=None,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    src_bucket = s3_resource.Bucket(src_buck)

    if src_is_dir and not src_path.endswith("/"):
        src_path = src_path + "/"

    print(f"Looking for objects with prefix: {src_path}")

    for obj in src_bucket.objects.filter(Prefix=f"{src_path}"):
        if obj.key.endswith(".zip"):
            filetype = "zip"
        elif obj.key.endswith(".tar"):
            filetype = "tar"
        elif obj.key.endswith(".tar.gz"):
            filetype = "tar.gz"
        else:
            filetype = None

        if filetype is not None:
            print("Extracting and uploading: ", obj.key)
            extract_upload(
                s3_resource,
                obj.Object(),
                dst_buck,
                upload_folder,
                verbose=verbose,
                filetype=filetype,
            )