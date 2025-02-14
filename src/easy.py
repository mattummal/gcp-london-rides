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
            start_station_id = element.get("start_station_id")
            end_station_id = element.get("end_station_id")

            if start_station_id is None or end_station_id is None:
                print(f"Missing station ID in element: {element}")
                return

            start_station = int(start_station_id)
            end_station = int(end_station_id)
            yield (start_station, end_station), 1
        except (TypeError, ValueError) as e:
            print(f"Error processing element {element}: {e}")


def run_easy_pipeline(output_path: str) -> None:
    """Runs the Beam pipeline to count rides and write the output to a text file.

    Args:
        output_path: The path to the output file where results will be written.
    """
    beam_options = PipelineOptions(
        runner="DataflowRunner",
        project="gcp-project-id",
        job_name="gcp-random-job-name",
        temp_location="gs://gcp-bucket-name/temp",
        region="europe-west1",
    )
    with beam.Pipeline(options=beam_options) as bp:
        (
            bp
            | "ReadDataFromBigQuery"
            >> beam.io.ReadFromBigQuery(
                query=(
                    "SELECT start_station_id, end_station_id "
                    "FROM `bigquery-public-data.london_bicycles.cycle_hire` "
                ),
                use_standard_sql=True,
            )
            | "FilterNulls"
            >> beam.Filter(
                lambda row: row["start_station_id"] is not None
                and row["end_station_id"] is not None
            )
            | "PairCount" >> beam.ParDo(RideCount())
            | "GroupBy" >> beam.CombinePerKey(sum)
            | "FormatOutput" >> beam.Map(lambda x: f"{x[0][0]},{x[0][1]},{x[1]}")
            | "WriteOutput"
            >> beam.io.WriteToText(
                file_path_prefix=output_path,
                file_name_suffix=".txt",
                shard_name_template="-SSSSS-of-NNNNN",
                num_shards=1,
            )
        )


if __name__ == "__main__":
    run_easy_pipeline("gs://gcp-bucket-name/test-output/output-file")
