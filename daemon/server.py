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
# Mailbox containing status from the Daemon.
_status_box = None


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

@_app.route("/status")
def _get_status():
  """ Gets a status report from the daemon. """
  logger.debug("Got HTTP request: %s" % (request.url))

  # Read the latest status.
  message = _status_box.peek()
  message["status"] = "okay"
  if not message:
    # No status was set yet.
    logger.error("Failed to get status: no status yet.")
    message = {"status": "error", "details": "No status available yet."}

  response = jsonify(message)
  response.status_code = 200
  return response


def _run_server(queue, status_box):
  """ Runs the flask server. Meant to be called in a different process.
  Args:
    queue: The queue to send requests for the main daemon on.
    status_box: A Mailbox that contains current status data from the daemon.
                This is so that the server can have quick access to this
                information. """
  global _queue
  global _status_box
  _queue = queue
  _status_box = status_box

  _app.run()

def start(queue, status_box):
  """ Starts the server running.
  Args:
    queue: The queue to send requests for the main daemon on.
    status_box: A Mailbox that contains current status data from the daemon.
                This is so that the server can have quick access to this
                information. """
  logger.info("Starting new server...")

  server = Process(target=_run_server, args=(queue, status_box))
  server.start()
