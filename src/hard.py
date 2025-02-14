import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    SetupOptions,
    WorkerOptions,
)

from geopy.distance import geodesic
from google.cloud import bigquery


PROJECT_ID = "gcp-project-id"
BUCKET_NAME = "gcp-bucket-name"


class ComputeDistance(beam.DoFn):
    def process(self, element, station_map):
        start_id, stop_id, ride_count = element

        if start_id in station_map and stop_id in station_map:
            start_coords = station_map[start_id]
            stop_coords = station_map[stop_id]
            distance_km = geodesic(start_coords, stop_coords).km
            total_distance = distance_km * ride_count
            yield f"{start_id},{stop_id},{ride_count},{total_distance}"


def run(project_id: str, bucket_name: str) -> None:
    options = PipelineOptions(
        project=project_id,
        temp_location=f"gs://{bucket_name}/temp",
        runner="DataflowRunner",
        region="europe-west1",
    )

    # Install dependencies on workers
    options.view_as(SetupOptions).requirements_file = "./requirements.txt"
    options.view_as(SetupOptions).setup_file = "./setup.py"
    options.view_as(SetupOptions).save_main_session = True

    # Improve worker scaling and machine performance
    worker_options = options.view_as(WorkerOptions)
    worker_options.num_workers = 5
    worker_options.max_num_workers = 20
    worker_options.machine_type = "n1-standard-4"

    client = bigquery.Client(project=project_id)
    station_query = """
    SELECT id, latitude, longitude 
    FROM `bigquery-public-data.london_bicycles.cycle_stations`
    """
    station_map = {
        row["id"]: (row["latitude"], row["longitude"])
        for row in client.query(station_query).result()
    }

    # Beam pipeline
    with beam.Pipeline(options=options) as bp:
        trips = (
            bp
            | "Read Trips"
            >> beam.io.ReadFromBigQuery(
                query="""
             SELECT start_station_id, end_station_id
             FROM `bigquery-public-data.london_bicycles.cycle_hire`
             """,
                use_standard_sql=True,
            )
            | "Pair and Count Rides"
            >> beam.Map(
                lambda row: ((row["start_station_id"], row["end_station_id"]), 1)
            )
            | "Aggregate Ride Counts" >> beam.CombinePerKey(sum)
            | "Format Data" >> beam.Map(lambda row: (row[0][0], row[0][1], row[1]))
            | "Compute Distance" >> beam.ParDo(ComputeDistance(), station_map)
            | "Write to GCS"
            >> beam.io.WriteToText(
                f"gs://{bucket_name}/test-output/test-hard-task",
                file_name_suffix=".txt",
                num_shards=1,
            )
        )


if __name__ == "__main__":
    run(PROJECT_ID, BUCKET_NAME)
