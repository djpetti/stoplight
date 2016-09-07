#!/usr/bin/python3


from urllib.request import Request, urlopen
from urllib.parse import quote_plus, urlencode
import argparse
import os


def _add_job(job_directory):
  """ Adds the job via the REST API.
  Args:
    job_directory: The job directory. """
  job_directory = os.path.abspath(job_directory)
  job_directory = quote_plus(job_directory)

  # Perform the request.
  url = "http://127.0.0.1:5000/add_job"
  values = {"job_dir": job_directory}

  data = urlencode(values)
  data = data.encode("ascii")
  request = Request(url, data)
  response = urlopen(request)

def main():
  # Parse arguments.
  parser = argparse.ArgumentParser( \
      description="Add a new job to the Stoplight queue.")
  parser.add_argument("job_directory", help="The path to the job directory.")

  args = parser.parse_args()

  _add_job(args.job_directory)


if __name__ == "__main__":
  main()
