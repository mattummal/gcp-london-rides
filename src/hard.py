import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pvalue import AsDict
from easy import RideCount
from geopy.distance import geodesic
from typing import Dict, Iterator, Tuple


class DistanceMeasurement(beam.DoFn):
    """A DoFn class for measuring distances between start and end stations."""

    def process(
        self,
        element: Tuple[Tuple[int, int], int],
        station_coords: Dict[int, Tuple[float, float]],
    ) -> Iterator[str]:
        (start_station, end_station), count = element

        print(f"Processing element: {element}")

        start_coords = station_coords.get(start_station)
        end_coords = station_coords.get(end_station)

        print(f"Start coords: {start_coords}, End coords: {end_coords}")

        if start_coords and end_coords:
            distance = geodesic(start_coords, end_coords).km
            total_distance = distance * count
            yield f"{start_station},{end_station},{count},{total_distance}"
        else:
            print(f"Missing coordinates for stations: {start_station}, {end_station}")


def run_hard_pipeline(output_path: str, beam_args: list) -> None:
    """Runs the Beam pipeline to measure distances and write the output to a text file.

    Args:
        output_path: The path to the output file where results will be written.
        beam_args: Additional arguments for configuring the Beam pipeline.
    """
    beam_options = PipelineOptions(
        beam_args,
        runner="DataflowRunner",
        project="gcp-project-id",
        job_name="unique-job-name",
        temp_location="gs://my-bucket/temp",
        region="eu-central1",
    )
    with beam.Pipeline(options=beam_options) as bp:
        station_coords = (
            bp
            | "Read Stations"
            >> beam.io.ReadFromBigQuery(
                query=(
                    "SELECT station_id, latitude, longitude "
                    "FROM `bigquery-public-data.london_bicycles.cycle_stations`"
                ),
                use_standard_sql=True,
            )
            | "Map Station Coords"
            >> beam.Map(lambda x: (x["station_id"], (x["latitude"], x["longitude"])))
            | "To Dict" >> beam.combiners.ToDict()
        )

        (
            bp
            | "ReadFromBigQuery"
            >> beam.io.ReadFromBigQuery(
                query=(
                    "SELECT start_station_id, end_station_id "
                    "FROM `bigquery-public-data.london_bicycles.cycle_hire`"
                ),
                use_standard_sql=True,
            )
            | "PairCount" >> beam.ParDo(RideCount())
            | "Group&Sum" >> beam.CombinePerKey(sum)
            | "DistanceMeasurement"
            >> beam.ParDo(DistanceMeasurement(), station_coords=AsDict(station_coords))
            | "WriteOut" >> beam.io.WriteToText(output_path, num_shards=1)
        )
