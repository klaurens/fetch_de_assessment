import ast
import hashlib
import json
import os
import subprocess
from datetime import datetime


def read_msg(url: str = "http://localhost:4566/000000000000/login-queue") -> bytes:
    """
    Reads the next message in the queue and returns the output.
    
    Args can be made into function arguments to generalize this function 
    and allow it to receive messages from other sources other than SQS.
    
    This function can also implement error handling in the future to fail fast.
    """
    args = ["awslocal", "sqs", "receive-message", "--queue-url", url]

    return subprocess.run(args, capture_output=True,).stdout


def encrypt(msg: str, length: int = 10) -> dict:
    """
    Encrypts `device_id` and `ip` fields by hashing their values.
    With `shake_256`, a certain length, in bytes, can be chosen. 
    I used default 10 as it is wholly legible by humans and differences are easily spotted 
    but is long enough to ensure that duplicates are unlikely.
    
    To expand on this function, error handling needs to be implemented to know what to do 
    when the input is not as expected. Alerting may also be set up when such case arise.
    
    `create_date` is assumed to be the date when the data is pulled from queue
    As the database expects `app_version` to be an integer, the periods are removed.
    `locale` can sometimes be None, which will be converted to string 'None'
    """
    dic = json.loads(msg)
    body = json.loads(dic["Messages"][0]["Body"])
    device_id = body["device_id"]
    ip = body["ip"]
    del body["device_id"]
    del body["ip"]
    body["masked_device_id"] = hashlib.shake_256(device_id.encode()).hexdigest(length)
    body["masked_ip"] = hashlib.shake_256(ip.encode()).hexdigest(length)
    body["app_version"] = body["app_version"].replace(".", "")
    body["locale"] = str(body["locale"])
    body["create_date"] = datetime.now().strftime("%Y-%m-%d")

    return body


def insert(
    load: dict,
    cols: tuple = (
        "user_id",
        "device_type",
        "masked_ip",
        "masked_device_id",
        "locale",
        "app_version",
        "create_date",
    ),
) -> None:
    """
    Writes data to Postgres.
    
    Further work could include deduplication and checking whether the data is already in the db or not.
    Generalizing the function by taking in different arguments to allow for usage to a different database / table.
    
    Writing the database password in the code should also be avoided and be moved to a config file. 
    For the purposes of this assessment I tried to keep it simple and easily understood thus I left the db password in.
    """

    col_string = str(cols).replace("'", "")
    ins = f"INSERT INTO user_logins {col_string} VALUES {tuple([load[col] for col in cols])};"
    subprocess.Popen(
        [
            "psql",
            "-d",
            "postgres",
            "-U",
            "postgres",
            "-p",
            "5432",
            "-h",
            "localhost",
            "-W",
            "-c",
            ins,
        ],
        env=dict(os.environ, PGPASSWORD="postgres"),
        stderr=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
    )

    return None


if __name__ == "__main__":
    # For the purposes of this assessment, everytime the .py file is called,
    # all 100 messages in the queue will be processed
    # Otherwise, I would implement a shell command parser to take in arguments

    for i in range(100):
        insert(encrypt(read_msg()))
