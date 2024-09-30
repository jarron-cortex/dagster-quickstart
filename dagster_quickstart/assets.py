import json
import requests
import pandas as pd
from sqlalchemy import create_engine
from dagster import AssetExecutionContext, MetadataValue, asset
from dagster_postgres import postgres_resource
from dagster_bigquery import bigquery_resource

POSTGRES_CONNECTION_STRING = "postgresql://username:password@host:port/database"

@asset(group_name="data_ingestion", compute_kind="Postgres to BigQuery")
def ingest_from_postgres(context: AssetExecutionContext) -> pd.DataFrame:
    """Ingest data from PostgreSQL."""
    # Connect to PostgreSQL
    engine = create_engine(POSTGRES_CONNECTION_STRING)
    query = "SELECT * FROM public.scorecard_service_results WHERE last_modified > '2022-01-01'"  # Modify as needed
    
    # Read data into a DataFrame
    df = pd.read_sql(query, engine)
    
    context.log.info(f"Retrieved {len(df)} records from PostgreSQL.")
    
    # Attach metadata
    context.add_output_metadata(
        {
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )
    
    return df

@asset(group_name="data_ingestion", compute_kind="BigQuery")
def write_to_bigquery(context: AssetExecutionContext, df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery."""
    if not df.empty:
        df.to_gbq("product_postgres_public.scorecard_service_results", 
                   project_id=context.resources.bigquery.project_id,
                   if_exists="append")  # Change to "append" to add new records
        
        context.log.info("Data written to BigQuery successfully.")
    else:
        context.log.info("No new records to write to BigQuery.")
        context.add_output_metadata({"message": "No new records to write."})

@job(resource_defs={
    "postgres": postgres_resource,
    "bigquery": bigquery_resource,
})
def data_ingestion_pipeline():
    """Pipeline to ingest data from PostgreSQL and write to BigQuery."""
    df = ingest_from_postgres()
    write_to_bigquery(df)
import pandas as pd

from dagster import (
    Config,
    MaterializeResult,
    MetadataValue,
    asset,
)


class HNStoriesConfig(Config):
    top_stories_limit: int = 10
    hn_top_story_ids_path: str = "hackernews_top_story_ids.json"
    hn_top_stories_path: str = "hackernews_top_stories.csv"


@asset
def hackernews_top_story_ids(config: HNStoriesConfig):
    """Get top stories from the HackerNews top stories endpoint."""
    top_story_ids = requests.get("https://hacker-news.firebaseio.com/v0/topstories.json").json()

    with open(config.hn_top_story_ids_path, "w") as f:
        json.dump(top_story_ids[: config.top_stories_limit], f)


@asset(deps=[hackernews_top_story_ids])
def hackernews_top_stories(config: HNStoriesConfig) -> MaterializeResult:
    """Get items based on story ids from the HackerNews items endpoint."""
    with open(config.hn_top_story_ids_path, "r") as f:
        hackernews_top_story_ids = json.load(f)

    results = []
    for item_id in hackernews_top_story_ids:
        item = requests.get(f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json").json()
        results.append(item)

    df = pd.DataFrame(results)
    df.to_csv(config.hn_top_stories_path)

    return MaterializeResult(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(str(df[["title", "by", "url"]].to_markdown())),
        }
    )
