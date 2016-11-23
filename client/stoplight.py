#!/usr/bin/python3


from urllib.request import Request, urlopen
from urllib.parse import quote_plus, urlencode
import argparse
import json
import os
import sys


_DAEMON_URL = "http://127.0.0.1:5000"


def _add_job(job_directory):
  """ Adds the job via the REST API.
  Args:
    job_directory: The job directory.
  Returns:
    0 if it succeeds. This is meant to be used as an exit code. """
  job_directory = os.path.abspath(job_directory)
  job_directory = quote_plus(job_directory)

  # Perform the request.
  url = os.path.join(_DAEMON_URL, "add_job")
  values = {"job_dir": job_directory}

  data = urlencode(values)
  data = data.encode("utf8")
  request = Request(url, data)
  response = urlopen(request)

  # See what the server told us.
  decoded = response.read().decode("utf8")
  message = json.loads(decoded)
  if message["status"] != "okay":
    # Something went wrong.
    print("Got error status from daemon: %s" % (message["details"]))
    return 1

  # We're all good.
  print("Job added successfully.")
  return 0

def _get_status():
  """ Gets a status report via the REST API.
  Returns:
    0 if it succeeds. This is meant to be used as an exit code. """
  # Perform the request.
  url = os.path.join(_DAEMON_URL, "status")
  response = urlopen(url)

  # Load and print the report.
  decoded = response.read().decode("utf8")
  message = json.loads(decoded)
  if message["status"] != "okay":
    # Something went wrong.
    print("Got error status from daemon %s" % (message["details"]))
    return 1

  running = message["running"]
  pending = message["pending"]

  print("Running Jobs: (%d)" % (len(running)))
  for name, description in running:
    print("\t%s (\"%s\")" % (name, description))
  print("Pending Jobs: (%d)" % (len(pending)))
  for name, description in pending:
    print("\t%s (\"%s\")" % (name, description))

  return 0

def main():
  # Parse arguments.
  parser = argparse.ArgumentParser( \
      description="Interact with the stoplight daemon.")
  parser.add_argument("-a", "--add_job",
                      help="Add a new job with this directory.")
  parser.add_argument("-s", "--status", action="store_true",
                      help="Get a brief status report from the daemon.")

  args = parser.parse_args()

  if args.add_job:
    # Add the job.
    sys.exit(_add_job(args.add_job))
  if args.status:
    # Get a status report.
    sys.exit(_get_status())

if __name__ == "__main__":
  main()
