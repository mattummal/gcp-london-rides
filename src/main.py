"""Main module for running Apache Beam pipelines based on specified tasks."""

import argparse
from easy import run_easy_pipeline
from hard import run_hard_pipeline


def parse_arguments() -> argparse.Namespace:
    """Parses command-line arguments.

    Returns:
        Parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Run the specified Beam pipeline task."
    )
    parser.add_argument("task", choices=["easy", "hard"], help="The task to execute.")
    parser.add_argument("output_path", help="The path to the output file.")
    parser.add_argument(
        "--beam-arg", action="append", default=[], help="Additional Beam arguments."
    )
    return parser.parse_args()


def main(task: str, output_path: str, beam_args: list) -> None:
    """Runs the specified pipeline task.

    Args:
        task: The task to be executed, either 'easy' or 'hard'.
        output_path: The path to the output file where results will be written.
        beam_args: Additional arguments for configuring the Beam pipeline.
    """
    if task == "easy":
        run_easy_pipeline(output_path, beam_args)
    elif task == "hard":
        run_hard_pipeline(output_path, beam_args)
    else:
        raise ValueError("Invalid task. Please choose 'easy' or 'hard'.")


if __name__ == "__main__":
    args = parse_arguments()
    main(args.task, args.output_path, args.beam_arg)
