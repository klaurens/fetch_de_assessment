# Fetch Rewards #
## Data Engineering Take Home: ETL off a SQS Qeueue ##

My solution to the problem is written in a Python file `pipeline.py`.
To run, `python3 pipeline.py` in terminal should process all 100 queued messages.


1) How will you read messages from the queue?
- I am reading the messages directly from subprocess. This is done to minimize any need for external packages that may or may not be available, and to keep things in one place.
2) What type of data structures should be used?
- To process the data, I used the `json` library to parse the message and kept everything largely as a python dictionary
3) How will you mask the PII data so that duplicate values can be identified?
- I used a hashing function so that any PII is obfuscated but duplicate values will still come out the same as it is a deterministic algorithm.
4) What will be your strategy for connecting and writing to Postgres?
- Using the subprocess library as well. By using standard python libraries, I can avoid dependency issues.
5) Where and how will your application run?
- To run, `python3 pipeline.py` in terminal
