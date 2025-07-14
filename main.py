import os
import tempfile
from datetime import datetime, timezone
from google.cloud import storage
import pandas as pd
from functions_framework import cloud_event

from stdatalog_core.HSD.HSDatalog import HSDatalog
import pyarrow as pa
import pyarrow.parquet as pq

storage_client = storage.Client()

@cloud_event
def process_dat_to_parquet(cloud_event):
    data = cloud_event.data
    bucket_name = data["bucket"]
    object_name = data["name"]

    if not object_name.endswith(".dat"):
        return

    prefix = os.path.dirname(object_name)
    tmpdir = tempfile.mkdtemp()
    local_folder = os.path.join(tmpdir, prefix.replace("/", "_"))
    os.makedirs(local_folder, exist_ok=True)

    bucket = storage_client.bucket(bucket_name)
    for blob in storage_client.list_blobs(bucket_name, prefix=prefix + "/"):
        dest = os.path.join(local_folder, os.path.basename(blob.name))
        blob.download_to_filename(dest)

    # ---------------- HSDatalog ----------------
    hsd = HSDatalog()
    print(f"[DEBUG] acquisition_folder contents:")
    print(os.listdir(local_folder))

    hsd_instance = hsd.create_hsd(acquisition_folder=local_folder)
    acq_info = HSDatalog.get_acquisition_info(hsd_instance)

    # 1) parse ISO8601 start_time
    start_time_str = acq_info.get("start_time", "1970-01-01T00:00:00.000Z")
    dt_start = datetime.fromisoformat(start_time_str.replace("Z", "+00:00"))

    fw_info = HSDatalog.get_firmware_info(hsd_instance)["firmware_info"]
    alias = fw_info.get("alias", "unknown")

    sensor = hsd.get_sensor(hsd_instance, "iis3dwb_acc")
    hsd.convert_dat_to_xsv(
        hsd_instance, sensor,
        start_time=0, end_time=-1,
        labeled=False, raw_data=False,
        output_folder=local_folder,
        file_format="PARQUET",
    )
    # -------------------------------------------

    parquet_in = os.path.join(local_folder, "iis3dwb_acc.parquet")
    df = pd.read_parquet(parquet_in)

    # 2) arricchimento Time e alias
    start_ns = int(dt_start.timestamp() * 1_000_000_000)
    df["Time"]  = df["Time"].astype("int64") + start_ns
    df["alias"] = alias
    df["Date"]  = dt_start.date()

    # 3) scrivi Parquet con delta encoding abilitato
    table = pa.Table.from_pandas(df)
    parquet_out = os.path.join(local_folder, "iis3dwb_acc_enriched.parquet")
    pq.write_table(
        table, parquet_out,
        compression="SNAPPY",
        data_page_version="2.0"
    )

    # 4) path di destinazione basato su dt_start
    dest_path = (
        f"{alias}/"
        f"{dt_start.year:04d}/{dt_start.month:02d}/{dt_start.day:02d}/"
        "iis3dwb_acc.parquet"
    )

    dest_blob = bucket.blob(dest_path)
    dest_blob.upload_from_filename(parquet_out)

    print(f"[OK] Caricato su gs://{bucket_name}/{dest_path}")
