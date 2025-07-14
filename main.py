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

# ------------- CONFIG -------------
SENSOR_NAME = "iis3dwb_acc"
REQUIRED_FILES = {
    "acquisition_info.json",
    "device_config.json",
    f"{SENSOR_NAME}.dat",
}

def all_required_present(blob_iter):
    """Ritorna True se nei blob c’è ogni file richiesto."""
    names = {os.path.basename(b.name).lower() for b in blob_iter}
    return REQUIRED_FILES.issubset(names), names


@cloud_event
def process_dat_to_parquet(cloud_event):
    data = cloud_event.data
    bucket_name = data["bucket"]
    object_name = data["name"]

    # ❌ Non filtriamo più sul solo ".dat": vogliamo controllare a ogni evento
    prefix = os.path.dirname(object_name)         # dati_raw/XYZ_20250711_...
    if not prefix:                                # ignora se il file è nella root
        return

    bucket = storage_client.bucket(bucket_name)
    blobs = list(storage_client.list_blobs(bucket_name, prefix=prefix + "/"))

    ready, present = all_required_present(blobs)
    if not ready:
        missing = REQUIRED_FILES - present
        print(f"[INFO] Cartella {prefix}: mancano {missing}, riproverò al prossimo evento")
        return                                      # uscita “soft” – la funzione verrà richiamata

    # ✅  ci sono tutti i file: procedi
    tmpdir = tempfile.mkdtemp()
    local_folder = os.path.join(tmpdir, "hsd_folder")
    os.makedirs(local_folder, exist_ok=True)
    for blob in blobs:
        dest = os.path.join(local_folder, os.path.basename(blob.name))
        blob.download_to_filename(dest)

    print("[DEBUG] File scaricati:", os.listdir(local_folder))

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
    # 2. Mappa “colonna → encoding”
    encodings = {"Time": "DELTA_BINARY_PACKED"}   # solo per Time

    # 3. Scrivi il Parquet
    parquet_out = os.path.join(local_folder, "iis3dwb_acc_enriched.parquet")
    with pq.ParquetWriter(
            parquet_out,
            table.schema,
            compression="SNAPPY",
            data_page_version="2.0",
            column_encoding=encodings) as writer:
        writer.write_table(table)

    # 4) path di destinazione basato su dt_start
    dest_path = (
        f"{alias}/"
        f"{dt_start.year:04d}/{dt_start.month:02d}/{dt_start.day:02d}/"
        "iis3dwb_acc.parquet"
    )

    dest_blob = bucket.blob(dest_path)
    dest_blob.upload_from_filename(parquet_out)

    print(f"[OK] Caricato su gs://{bucket_name}/{dest_path}")
