import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json


# Constants (Production best practice)
PROJECT_ID = "burnished-web-484613-t0"
REGION = "us-central1"
BUCKET = "gs://svdataflow_bucket"

SUBSCRIPTION = f"projects/{PROJECT_ID}/subscriptions/house-subscription"
TABLE = f"{PROJECT_ID}:house_dataset.house_prices_stream"


# Parse incoming Pub/Sub message
class ParseHouseData(beam.DoFn):

    def process(self, element):

        data = json.loads(element.decode("utf-8"))

        yield {
            "price": float(data["price"]),
            "bedrooms": int(data["bedrooms"]),
            "bathrooms": float(data["bathrooms"]),
            "sqft_living": int(data["sqft_living"]),
            "floors": int(data["floors"])
        }


def run():

    # Clean PipelineOptions format (your preferred format)
    options = PipelineOptions(

        # Required for streaming
        streaming=True,

        # Runner
        runner="DataflowRunner",

        # Project settings
        project=PROJECT_ID,
        region=REGION,

        # Required GCS locations
        temp_location=f"{BUCKET}/temp",
        staging_location=f"{BUCKET}/staging",

        # Job name
        job_name="house-streaming-job",

        # Production recommended options
        save_main_session=True,

        # Worker configuration
        machine_type="n1-standard-1",
        max_num_workers=3,

        # Autoscaling
        autoscaling_algorithm="THROUGHPUT_BASED"

    )


    with beam.Pipeline(options=options) as pipeline:

        (
            pipeline

            | "Read from PubSub"
            >> beam.io.ReadFromPubSub(
                subscription=SUBSCRIPTION
            )

            | "Parse Data"
            >> beam.ParDo(ParseHouseData())

            | "Write to BigQuery"
            >> beam.io.WriteToBigQuery(
                table=TABLE,

                schema="""
                price:FLOAT,
                bedrooms:INTEGER,
                bathrooms:FLOAT,
                sqft_living:INTEGER,
                floors:INTEGER
                """,

                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )


if __name__ == "__main__":
    run()
