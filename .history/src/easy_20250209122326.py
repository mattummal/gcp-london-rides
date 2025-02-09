import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from typing import Dict, Iterator, Tuple


class RideCount(beam.DoFn):
    """A DoFn class for counting rides between start and end stations."""

    def process(self, element: Dict[str, str]) -> Iterator[Tuple[Tuple[int, int], int]]:
        """Processes each element to extract start and end station IDs.

        Args:
            element: A dictionary containing 'start_station_id' and 'end_station_id'.

        Yields:
            A key-value pair where the key is a tuple of start and end station IDs,
            and the value is 1, representing a single ride.
        """
        try:
            start_station = int(element["start_station_id"])
            end_station = int(element["end_station_id"])
            yield (start_station, end_station), 1
        except ValueError as e:
            print(f"Error converting station IDs to integers: {element} - {e}")
        except KeyError as e:
            print(f"Missing expected field in element: {element} - {e}")


def run_easy_pipeline(output_path: str, beam_args: list) -> None:
    """Runs the Beam pipeline to count rides and write the output to a text file.

    Args:
        output_path: The path to the output file where results will be written.
        beam_args: Additional arguments for configuring the Beam pipeline.
    """
    beam_options = PipelineOptions(
        beam_args,
        runner="DataflowRunner",
        project="project-id",
        job_name="unique-job-name",
        temp_location="gs://my-bucket/temp",
        region="eu-central1",
    )
    with beam.Pipeline(options=beam_options) as bp:
        (
            bp
            | "ReadDataFromBigQuery"
            >> beam.io.ReadFromBigQuery(
                query=(
                    "SELECT start_station_id, end_station_id "
                    "FROM `bigquery-public-data.london_bicycles.cycle_hire`"
                ),
                use_standard_sql=True,
            )
            | "Pair&Count" >> beam.ParDo(RideCount())
            | "GroupBy" >> beam.CombinePerKey(sum)
            | "FormatOutput" >> beam.Map(lambda x: f"{x[0][0]},{x[0][1]},{x[1]}")
            | "WriteOutput" >> beam.io.WriteToText(output_path, num_shards=1)
        )
