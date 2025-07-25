import os
import tempfile
from datetime import datetime, timezone
from google.cloud import storage
import pandas as pd
from functions_framework import cloud_event
import numpy as np

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

    df.rename(columns=lambda col: (
    col.replace(" ", "_")
       .replace("[", "")
       .replace("]", "")
       .replace("(g)", "")
       .replace("{g}", "")
       .replace("/g", "")
       .replace("°", "deg")
       .replace(",", "_")
    ), inplace=True)


    # 2) arricchimento Time e alias
    start_ns = int(dt_start.timestamp() * 1_000_000_000)
    time_rel_ns = (df["Time"].astype("float64") * 1_000_000_000).round().astype("int64")
    df["Time"]  = time_rel_ns + start_ns                   # Time assoluto in ns
    df["alias"] = alias
    # df["Date"]  = dt_start.date()

    # 3) scrivi Parquet con delta encoding abilitato
    table = pa.Table.from_pandas(df)
    # 2. Mappa “colonna → encoding”

    # 3. Scrivi il Parquet
    parquet_out = os.path.join(local_folder, "iis3dwb_acc_enriched.parquet")
    enc = {"Time": "DELTA_BINARY_PACKED"}
    pq.write_table(
        table, parquet_out,
        compression="SNAPPY",
        data_page_version="2.0",
        column_encoding=enc,
        use_dictionary=["alias"]   # lista di colonne da tenere in dizionario
    )

    timestamp_str = dt_start.strftime("%Y%m%d_%H%M%S")

    # 4) path di destinazione basato su dt_start
    dest_raw  = (
        f"data_parquet/"
        f"alias={alias}/"
        f"year={dt_start.year:04d}/"
        f"month={dt_start.month:02d}/"
        f"day={dt_start.day:02d}/"
        f"iis3dwb_acc_{timestamp_str}.parquet"
    )


    dest_blob = bucket.blob(dest_raw )
    dest_blob.upload_from_filename(parquet_out)

    print(f"[OK] Caricato su gs://{bucket_name}/{dest_raw }")

    # =================================================================
    # =============  RMS SU FINESTRE PIENE DA 1 s  ====================
    # =================================================================



    # 1) bucket da 1 s basato su Time assoluto (Time è già in ns)
    df["bucket_s"] = df["Time"] // 1_000_000_000

    # 2) individua i bucket completi in base alla DURATA (≥ 0,999 s)
    groups = df.groupby("bucket_s")
    full_buckets = [
        b for b, g in groups
        if (g["Time"].max() - g["Time"].min()) >= 0.999 * 1_000_000_000
    ]
    if not full_buckets:
        print("[WARN] Nessuna finestra di 1 s completa: RMS non scritto")
        return

    df_full = df[df["bucket_s"].isin(full_buckets)].copy()

    # 3) RMS per asse (x, y, z)
    ACC_COLS = ["A_x_g", "A_y_g", "A_z_g"]          # <<-- aggiorna se diverso
    rms_df = (
        df_full
        .groupby("bucket_s")[ACC_COLS]
        .agg(lambda a: np.sqrt((a**2).mean()))
        .reset_index()
        .rename(columns={
            "A_x_g": "rms_x",
            "A_y_g": "rms_y",
            "A_z_g": "rms_z",
        })
    )

    # (Facoltativo) RMS della magnitudine, utile come vibrazione complessiva
    rms_df["rms_mod"] = np.sqrt((rms_df[["rms_x", "rms_y", "rms_z"]]**2).sum(axis=1))

    # 4) colonne extra di contesto

    rms_df["Time"]  = (rms_df["bucket_s"] * 1_000_000_000).round().astype("int64")  
    rms_df["alias"] = alias          
    rms_df.drop(columns=["bucket_s"], inplace=True)
                     


    # 5) scrivi Parquet con delta-encoding su Time
    parquet_rms = os.path.join(local_folder, f"{SENSOR_NAME}_rms.parquet")
    pq.write_table(
        pa.Table.from_pandas(rms_df),
        parquet_rms,
        compression="SNAPPY",
        data_page_version="2.0",
        column_encoding={"Time": "DELTA_BINARY_PACKED"},
        use_dictionary=["alias"],
    )

    # 6) upload in directory Hive-like separata
    dest_rms = (
        f"data_parquet/"
        f"alias={alias}/"
        f"year={dt_start.year:04d}/"
        f"month={dt_start.month:02d}/"
        f"day={dt_start.day:02d}/"
        f"{SENSOR_NAME}_rms_{timestamp_str}.parquet"
    )
    

    dest_blob = bucket.blob(dest_rms )
    dest_blob.upload_from_filename(parquet_rms)

    print(f"[OK] Caricato su gs://{bucket_name}/{dest_raw }")