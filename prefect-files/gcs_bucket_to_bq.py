from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import sys
import re


@task(retries=3)
def extract_from_gcs(file_name: str) -> pd.DataFrame():
    """Download trip data from GCS"""
    gcs_path = f"{file_name}.parquet"
    gcs_block = GcsBucket.load("economic-indicators-gcs-bucket-block")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"data_modeling_project_bucket/economic_indicators")
    
    df = pd.read_parquet(Path(f"{gcs_path}"))

    return(df)


@task()
def transform(df: pd.DataFrame) -> pd.DataFrame:
    
    col_list = list(df.columns)

    for i in range(len(col_list)):
        """formatting to allow the BQ headers to be read in"""
        col_list[i] = 'A' + re.sub(r'[^a-zA-Z0-9_]+', '', col_list[i])

    df.columns = col_list

    
    return df



@task()
def write_bq(df: pd.DataFrame, file_name: str) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("gcp-credentials")

    
    df.to_gbq(
        destination_table=f"economic_indicators.{file_name}",
        project_id="data-modeling-project-405019",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="replace",
    )
    


@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""
    file_name = sys.argv[1]

    df = extract_from_gcs(file_name)

    df = transform(df)
    
    #df = transform(path)
    write_bq(df, file_name)


if __name__ == "__main__":
    etl_gcs_to_bq()