"""
Daily Feature Engineering Pipeline.

Produces daily feature sets for flight traffic forecasting:
- Hourly flight counts with time-based and lag features
- Uploads to S3 for model training
"""

__version__ = "0.1.0"
