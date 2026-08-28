"""
Feature engineering entry point.

Delegates to the real pipeline runner in src/pipeline/run.py.
"""

from src.pipeline.run import main

if __name__ == "__main__":
    main()
