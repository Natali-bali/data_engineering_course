import pandas as pd
import sys
#some fansy stuff with pandas
print(sys.argv)
day = sys.argv[1] #I can pass day as an argument when run the docker container: docker run -it test:pandas 2021-01-20. can pass arguments with <space>

print(f"Everything works for day {day}")