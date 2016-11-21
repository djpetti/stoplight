from multiprocessing import Process
import json
import logging
import urllib

from flask import abort, Flask, g, jsonify, request

import job


""" A minimal server that will implement the REST API that we use to communicate
with the daemon. """


logger = logging.getLogger(__name__)


# Flask app.
_app = Flask(__name__)
# Queue we use to send data to the daemon
_queue = None


@_app.route("/add_job", methods=["POST"])
def _add_job():
  """ Adds a new job to the daemon. """
  logger.debug("Got HTTP request: %s" % (request.url))

  job_dir = request.form.get("job_dir", None)
  if not job_dir:
    logger.error("Invalid request with no job_dir parameter.")
    abort(400)

  job_dir = urllib.parse.unquote_plus(job_dir)

  # Interpret the job configuration.
  message = {"status": "okay"}
  try:
    new_job = job.Job(job_dir)

    # Actually add the job.
    _queue.put({"type": "add_job", "job": new_job})
  except job.ConfigurationError as error:
    # Bad configuration. Return an error to the user.
    logger.error("Failed to add job: %s" % str(error))
    message = {"status": "error", "details": str(error)}

  response = jsonify(message)
  response.status_code = 200
  return response


def _run_server(queue):
  """ Runs the flask server. Meant to be called in a different process.
  Args:
    queue: The queue to send requests for the main daemon on. """
  global _queue
  _queue = queue

  _app.run()

def start(queue):
  """ Starts the server running.
  Args:
    queue: The queue to send requests for the main daemon on. """
  logger.info("Starting new server...")

  server = Process(target=_run_server, args=(queue,))
  server.start()
