import logging
import os

import yaml
try:
  # Use libyaml, if available.
  from yaml import CLoader as Loader
except ImportError:
  # Otherwise, fall back on Python version.
  from yaml import Loader

from util import ConfigurationError
import docker


logger = logging.getLogger(__name__)


class Job:
  """ Represents a single job. """

  class ResourceUsage:
    """ Stores and validates data about job resource requirements. """
    def __init__(self, resource_data):
      """
      Args:
        resource_data: The raw ResourceUsage section from the YAML file to
        initialize with. """

      """ CPU requirements. """
      self.Cpu = None
      """ RAM requirements. """
      self.Ram = None
      """ GPU requirements. """
      self.Gpu = None
      """ VRAM requirements. """
      self.Vram = None

      if not resource_data:
        # No ResourceUsage section.
        logger.warning("No ResourceUsage section found in job.yaml.")
        return

      # Otherwise, initialize from the data we have.
      self.__initialize_from_data(resource_data)

    def __initialize_from_data(self, resource_data):
      """ Initialize the class based on an existing ResourceUsage section. """
      for pair in resource_data:
        if "CpuUsage" in pair:
          self.Cpu = pair["CpuUsage"]
        elif "RamUsage" in pair:
          self.Ram = pair["RamUsage"]
        elif "GpuUsage" in pair:
          self.Gpu = pair["GpuUsage"]
        elif "VramUsage" in pair:
          self.Vram = pair["VramUsage"]

        else:
          logger.warning("Got unknown resource: '%s'." % (pair))

      if not self.Cpu:
        # Assume insignificant CPU load.
        logger.debug("Got no CpuUsage section, asuming 0.")
        self.Cpu = 0

      if not self.Ram:
        # Assume insignificant RAM usage.
        logger.debug("Got no RamUsage section, asuming 0.")
        self.Ram = 0

      if not self.Gpu:
        # Assume insignificant GPU usage.
        logger.debug("Got no GpuUsage section, asuming 0.")
        self.Gpu = 0

      if not self.Vram:
        # Assume insignificant VRAM usage.
        logger.debug("Got no VramUsage section, asuming 0.")
        self.Vram = 0


  def __init__(self, job_directory):
    """
    Args:
      job_directory: The path to the job directory. """
    self.__job_directory = job_directory

    # Open files for output.
    out_file_path = os.path.join(self.__job_directory, "job.out")
    err_file_path = os.path.join(self.__job_directory, "job.err")
    self.__out_file = None
    self.__err_file = None

    # Interpret the job configuration.
    self.__interpret_configuration()

  def __del__(self):
    # Close output files.
    if self.__out_file:
      self.__out_file.close()
    if self.__err_file:
      self.__err_file.close()

  def __str__(self):
    """ Returns:
      The job's name. """
    return self.__name

  def __interpret_configuration(self):
    def missing_param(name):
      """ Small helper function to raise a missing parameter exception.
      Args:
        name: The name of the missing parameter. """
      raise ConfigurationError( \
          "Invalid job.yaml: '%s' parameter is required." % (name))

    # Read the configuration file.
    config_path = os.path.join(self.__job_directory, "job.yaml")
    if not os.path.exists(config_path):
      raise ConfigurationError("Could not find job.yaml file in %s!" % \
                               (self.__job_directory))

    config_data = None
    with open(config_path) as config_file:
      config_data = yaml.load(config_file, Loader=Loader)

    # Set the proper attributes from the config file.
    self.__name = config_data.get("Name")
    if not self.__name:
      missing_param("Name")

    self.__description = config_data.get("Description")
    if not self.__description:
      missing_param("Description")

    self.__container_name = config_data.get("Container")
    if not self.__container_name:
      missing_param("Container")

    # Optional volumes.
    self.__volumes = config_data.get("Volumes", {})

    # Handle the ResourceUsage section.
    self.__resource_usage = Job.ResourceUsage(config_data.get("ResourceUsage"))

  def start(self):
    """ Starts the job running. """
    logger.info("Starting job: %s (%s)", self.__name, self.__description)

    self.__out_file = open(out_file_path, "a")
    self.__err_file = open(err_file_path, "a")

    # First, create the docker container to run inside.
    self.__container = docker.Container(self.__container_name,
                                        self.__job_directory,
                                        volumes=self.__volumes)
    # Run the script to start the job.
    self.__container.run_exe("run_job.sh")

  def is_finished(self):
    """
    Returns:
      True if the job is finished running, False otherwise. """
    return self.__container.is_finished()

  def get_name(self):
    """
    Returns:
      The name of the job. """
    return self.__name

  def get_resource_usage(self):
    """
    Returns:
      The resource usage for this job. """
    return self.__resource_usage

  def write_job_output(self):
    """ Writes the stdout and stderr streams from the job to files in the job
    directory called job.out and job.err respectively. """
    new_output = self.__container.get_output()
    new_errors = self.__container.get_error()

    if new_output:
      self.__out_file.write(new_output)
      # Flush so we can tail -f it in realtime.
      self.__out_file.flush()

    if new_errors:
      self.__err_file.write(new_errors)
      self.__err_file.flush()
