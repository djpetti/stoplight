#!/usr/bin/python3


import logging
import time


from manager import Manager


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

  # Create and run the manager.
  manager = Manager()
  while True:
    manager.update()
    time.sleep(5)


if __name__ == "__main__":
  main()
