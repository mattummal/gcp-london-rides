import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from typing import Dict, Iterator, Tuple
import csv
import io


class RideCount(beam.DoFn):
    """A DoFn class for counting rides between start and end stations."""

    def process(self, element: Dict[str, str]) -> Iterator[Tuple[Tuple[int, int], int]]:
        """Processes each element to extract start and end station IDs.

        Args:
            element: A dictionary containing 'Start Station ID' and 'End Station ID'.

        Yields:
            A key-value pair where the key is a tuple of start and end station IDs,
            and the value is 1, representing a single ride.
        """
        try:
            start_station = int(element["Start Station ID"])
            end_station = int(element["End Station ID"])
            yield (start_station, end_station), 1
        except ValueError as e:
            print(f"Error converting station IDs to integers: {element} - {e}")
        except KeyError as e:
            print(f"Missing expected field in element: {element} - {e}")


def parse_csv_line(line: str) -> Dict[str, str]:
    """Parses a CSV line into a dictionary.

    Args:
        line: A string representing a line in the CSV file.

    Returns:
        A dictionary with keys 'Start Station ID' and 'End Station ID'.
    """
    try:
        reader = csv.DictReader(
            io.StringIO(line),
            fieldnames=[
                "Journey Duration",
                "Journey ID",
                "End Date",
                "End Month",
                "End Year",
                "End Hour",
                "End Minute",
                "End Station ID",
                "Start Date",
                "Start Month",
                "Start Year",
                "Start Hour",
                "Start Minute",
                "Start Station ID",
            ],
        )
        result = next(reader)
        print(f"Parsed line into: {result}")
        return result
    except Exception as e:
        print(f"Error parsing line: {line} - {e}")
        return {}


def run_easy_pipeline(input_path: str, output_path: str) -> None:
    """Runs the Beam pipeline to count rides and write the output to a text file.

    Args:
        input_path: The path to the input CSV file containing ride data.
        output_path: The path to the output file where results will be written.
    """
    beam_options = PipelineOptions(runner="DirectRunner")
    with beam.Pipeline(options=beam_options) as bp:
        (
            bp
            | "ReadDataFromFile"
            >> beam.io.ReadFromText(input_path, skip_header_lines=1)
            | "ParseCSV" >> beam.Map(parse_csv_line)
            | "Pair&Count" >> beam.ParDo(RideCount())
            | "GroupBy" >> beam.CombinePerKey(sum)
            | "FormatOutput" >> beam.Map(lambda x: f"{x[0][0]},{x[0][1]},{x[1]}")
            | "WriteOutput" >> beam.io.WriteToText(output_path, num_shards=1)
        )


if __name__ == "__main__":
    run_easy_pipeline("data/ride_data.csv", "data/easy_output_file.txt")
