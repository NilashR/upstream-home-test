"""Pipeline execution scripts for the medallion architecture.

This package contains executable scripts for running the different layers
of the medallion architecture data pipeline.

Scripts:
    bronze_ingest.py: Fetches raw data from API and stores in Bronze layer
    silver_transform.py: Transforms Bronze data and stores in Silver layer
    run_pipeline.py: Runs the complete pipeline from Bronze to Silver
"""
