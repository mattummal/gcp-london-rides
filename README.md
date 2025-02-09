# London Bicycles Data Engineering Challenge

This project is a data engineering challenge that processes public datasets on London bicycles to gain insights into cycling behaviour using Apache Beam and Google Cloud Dataflow.

## Project Structure

- `easy.py`: Contains the logic for the easy task, which calculates the number of rides between each pair of stations.
- `hard.py`: Contains the logic for the hard task, which calculates the total distance traveled for rides between stations.
- `main.py`: The entry point for running the pipelines. It accepts command-line arguments to specify the task and output path.

## Setup Instructions

### Prerequisites

- Python 3.8 or later
- Google Cloud SDK
- Apache Beam
- Geopy

### Setting Up the Environment

1. **Create a Virtual Environment:**

   ```bash
   python3 -m venv venv
   ```

2. **Activate the Virtual Environment:**

   - On Windows:
     ```bash
     venv\Scripts\activate
     ```
   - On macOS and Linux:
     ```bash
     source venv/bin/activate
     ```

3. **Install Dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

### Running the Pipelines

1. **Run the Easy Task:**

   ```bash
   python main.py easy gs://your-bucket/output/easy_task.txt
   ```

2. **Run the Hard Task:**

   ```bash
   python main.py hard gs://your-bucket/output/hard_task.txt
   ```

Replace `gs://your-bucket/output/easy_test` and `gs://your-bucket/output/hard_test` with your actual Google Cloud Storage paths.
