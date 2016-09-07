from fcntl import fcntl, F_GETFL, F_SETFL
import logging
import os
import subprocess
import sys

import util


""" A simple wrapper that makes interacting with docker less ugly. """


logger = logging.getLogger(__name__)


class Container:
  """ Represents a running container. """

  def __init__(self, container, job_dir, nvidia=True):
    """
    Args:
      container: The container to run.
      job_dir: The directory that will be mounted under /job_files in the
      container.
      nvidia: If true, it will use nvidia-docker instead of normal docker. """
    self.__container = container
    self.__job_dir = os.path.abspath(job_dir)

    self.__process = None

    if nvidia:
      self.__docker = util.get_path("nvidia-docker")
    else:
      self.__docker = util.get_path("docker")

  def __del__(self):
    # Stop the processes.
    if (self.__process and not self.is_finished()):
      logger.warning("Terminating job from '%s.'" % (self.__job_dir))
      self.__process.terminate()

  def run_exe(self, exe):
    """ Runs an executable in the container. The executable is run in the
    context of the root directory of the container.
    Args:
      exe: The name of the executable to run. It is asumed that this is located
      in job_dir. """
    local_exe_path = os.path.join(self.__job_dir, exe)
    logger.debug("Running '%s' in container '%s'.", exe, self.__container)
    if not os.path.exists(local_exe_path):
      raise util.ConfigurationError("'%s' not found, or not executable." % \
                                    (local_exe_path))

    # Make the docker command.
    exe_path = os.path.join("job_files", exe)
    command = [self.__docker, "run", "--rm", "--net=host", "-v",
               "%s:/job_files" % (self.__job_dir), self.__container, exe_path]
    logger.debug("Running command: %s" % (command))
    # Run the command.
    self.__process = subprocess.Popen(command, stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE)
    # Use non-blocking IO.
    flags = fcntl(self.__process.stdout, F_GETFL)
    fcntl(self.__process.stdout, F_SETFL, flags | os.O_NONBLOCK)
    flags = fcntl(self.__process.stderr, F_GETFL)
    fcntl(self.__process.stderr, F_SETFL, flags | os.O_NONBLOCK)

  def is_finished(self):
    """
    Returns:
      True if the container is finished executing, False otherwise. """
    retcode = self.__process.poll()
    if retcode == None:
      # Not done yet.
      return False

    if retcode != 0:
      raise RuntimeError("Internal process exited with status %d." \
                         " Nice going, nerd!" % (retcode))

    logger.info("Job from %s finished successfully." % (self.__job_dir))
    return True

  def get_output(self):
    """
    Returns:
      The most recent output from stdout. """
    output = ""

    while True:
      read = self.__process.stdout.read(1024)
      if not read:
        break
      output += read.decode("utf-8")

    return output

  def get_error(self):
    """
    Returns:
      The most recent output from stderr. """
    output = ""

    while True:
      read = self.__process.stderr.read(1024)
      if not read:
        break
      output += read.decode("utf-8")

    return output
