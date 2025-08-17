

# Data Pipeline Mental Model
The idea of a pipeline is how data engineers design and build these systems. Tracking the state of each piece of work as it moves through a series of steps and being able to safely resume the process if it fails is the core principle behind professional ETL (Extract, Transform, Load) pipelines.

Let's break down how the features to map them to the "pipeline" concept:

## Extraction (The Start of the Pipeline):

The scraper runs and finds new posts. It saves the raw, unprocessed data to the data/raw/ directory. This is like placing raw materials at the very beginning of an assembly line.

## Transformation (The Middle of the Pipeline):

Entering the Workspace: When a batch job is needed, the orchestrator saves the specific posts to be enriched into the workspace/ directory (e.g., unsubmitted_posts_chunk_1.jsonl). This is like moving the raw materials to a specific workstation.

Work in Progress: Once the job is successfully submitted to the Gemini API, the file is renamed (e.g., to temp_posts_chunk_1.jsonl) and its Job ID is logged in pending_jobs.json. This is our tracking system. We now know exactly which piece of work is at which stage of the assembly line.

Resilience: if the script fails at any point, these files in the workspace/ act as a persistent "to-do list," allowing the pipeline to restart exactly where it left off on the next run.

## Loading (The End of the Pipeline):

The `--check-job` and `--export` commands act as the final quality control and shipping steps. They take the finished work from the "Transformation" station, update the final product (the data/processed/ directory), and then clean up the workstation (workspace/).