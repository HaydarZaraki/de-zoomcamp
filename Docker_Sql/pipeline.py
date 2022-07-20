import sys

import pandas as pd

#sys.argv is used for specifying the arguments passed with the file when running the container
print(sys.argv)

# First Argument sys.argv[0] by default is "pipeline.py"
# Second argument sys.argv[1] is the day of execution

day = sys.argv[1]

print(f"Pipeline Built Successfully at {day}")