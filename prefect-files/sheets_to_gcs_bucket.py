from pathlib import Path
import pandas as pd
from prefect import flow, task
import sys
from prefect_gcp.cloud_storage import GcsBucket

@task()#(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read data from google sheets into pandas DataFrame"""
    #NOTE: MAKE SURE TO PUT PARENTHESES "" AROUND THE URL OR IT DOESN'T WORK!!!
    df = pd.read_csv(dataset_url, sep='\t')
    df['RowNumber'] = df.reset_index().index + 1
    #Maybe put an assert statement in here later
    print('len(df.columns ' + str(len(df.columns)))
    return df

@task()
def write_local(df: pd.DataFrame, dataset_name: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"{dataset_name}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("economic-indicators-gcs-bucket-block")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return

@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    dataset_name = sys.argv[1]
    dataset_url = sys.argv[2]

    df = fetch(dataset_url)
    path = write_local(df, dataset_name)
    write_gcs(path)


if __name__ == "__main__":
    etl_web_to_gcs()