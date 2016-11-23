#!/usr/bin/python3


from multiprocessing import Queue
from queue import Empty
import logging
import time


from manager import Manager

import messaging
import server


""" Main file for the stoplight daemon. """


def init_logging(logfile):
  """ Initializes logging.
  Args:
    logfile: File that stuff will be logged to. """
  root = logging.getLogger()
  root.setLevel(logging.DEBUG)

  file_handler = logging.FileHandler(logfile)
  file_handler.setLevel(logging.DEBUG)
  stream_handler = logging.StreamHandler()
  stream_handler.setLevel(logging.DEBUG)

  formatter = logging.Formatter("%(name)s@%(asctime)s: " +
      "[%(levelname)s] %(message)s")
  file_handler.setFormatter(formatter)
  stream_handler.setFormatter(formatter)

  root.addHandler(file_handler)
  root.addHandler(stream_handler)

def main():
  # Initialize logging.
  init_logging("stoplightd.log")

  # Start the server.
  server_queue = Queue()
  status_box = messaging.Mailbox()
  server.start(server_queue, status_box)

  # Create and run the manager.
  manager = Manager()
  while True:
    # Check for new jobs and add them.
    try:
      command = server_queue.get(block=False)
      if command["type"] == "add_job":
        # Add the job.
        manager.add_job(command["job"])

    except Empty:
      pass

    manager.update()

    # Set the status after the update.
    status_box.set(manager.get_status())

    time.sleep(5)


if __name__ == "__main__":
  main()
