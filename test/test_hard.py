import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pvalue import AsDict
from geopy.distance import geodesic
from typing import Dict, Iterator, Tuple
import csv
import pandas as pd
import io
from test_easy import RideCount


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


def parse_station_coords(file_path: str) -> Iterator[Tuple[int, Tuple[float, float]]]:
    """Reads a CSV file using pandas and yields station ID and coordinates."""
    try:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path)

        # Iterate over each row in the DataFrame
        for _, row in df.iterrows():
            station_id = int(row["Station ID"])
            latitude = float(row["Latitude"])
            longitude = float(row["Longitude"])
            yield station_id, (latitude, longitude)
    except Exception as e:
        print(f"Error reading CSV file: {e}")


def parse_ride_data(line: str) -> Dict[str, str]:
    """Parses a CSV line into ride data.

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
        record = next(reader)
        return {
            "Start Station ID": record["Start Station ID"],
            "End Station ID": record["End Station ID"],
        }
    except Exception as e:
        print(f"Error parsing ride data: {line} - {e}")
        return {}


def run_hard_pipeline(
    input_path: str, station_coords_path: str, output_path: str
) -> None:
    """Runs the Beam pipeline to measure distances and write the output to a text file.

    Args:
        input_path: The path to the input CSV file containing ride data.
        station_coords_path: The path to the input CSV file containing station coordinates.
        output_path: The path to the output file where results will be written.
    """
    beam_options = PipelineOptions(runner="DirectRunner")
    with beam.Pipeline(options=beam_options) as bp:
        station_coords = (
            bp
            | "Read Station Coords"
            >> beam.io.ReadFromText(station_coords_path, skip_header_lines=1)
            | "Parse Station Coords" >> beam.Map(parse_station_coords)
            | "Filter None Coords" >> beam.Filter(lambda x: x is not None)
            | "Debug Station Coords" >> beam.Map(print)  # Debugging step
            | "To Dict" >> beam.combiners.ToDict()
        )
        (
            bp
            | "Read Ride Data" >> beam.io.ReadFromText(input_path, skip_header_lines=1)
            | "Parse Rides" >> beam.Map(parse_ride_data)
            | "Filter Empty" >> beam.Filter(lambda x: x)
            | "PairCount" >> beam.ParDo(RideCount())
            | "Group&Sum" >> beam.CombinePerKey(sum)
            | "DistanceMeasurement"
            >> beam.ParDo(DistanceMeasurement(), station_coords=AsDict(station_coords))
            | "WriteOut" >> beam.io.WriteToText(output_path, num_shards=1)
        )


if __name__ == "__main__":
    run_hard_pipeline(
        "data/ride_data.csv", "data/station_coords.csv", "data/hard_output_file.txt"
    )
