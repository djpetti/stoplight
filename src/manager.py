from collections import deque
from multiprocessing import cpu_count
import logging
import os
import shutil

import psutil

import job
import nvidia


""" Manages a set of jobs, and determines how to best use the resources of a
machine. """


logger = logging.getLogger(__name__)


class Manager:
  def __init__(self, job_location):
    """
    Args:
      job_location: Where to store files for a job. """
    # This is the queue that keeps track of our pending jobs.
    self.__pending_jobs = deque()
    # This is the set that keeps track of our running jobs.
    self.__running_jobs = set()
    # Location where data for pending jobs is stored.
    self.__job_location = job_location

    # TODO(danielp): Support for multiple GPUs.
    self.__gpu = nvidia.Gpu(0)

    # Current usage percentages for each of the resources.
    self.__cpu_usage = 0
    self.__ram_usage = 0
    self.__gpu_usage = 0
    self.__vram_usage = 0

    # The current smallest resource usage that a particular job has.
    self.__smallest_cpu = 100
    self.__smallest_ram = 100
    self.__smallest_gpu = 100
    self.__smallest_vram = 100

  def __get_available_resources(self):
    """ Gets the available amount of certain system resources. """
    self.__cpu_cores = cpu_count()
    self.__total_ram = psutil.virtual_memory().total
    self.__total_vram = self.__gpu.get_total_vram()

    logger.info("Running with %d CPU cores, and %d bytes of RAM." % \
                (self.__cpu_cores, self.__total_ram))
    logger.info("Got GPU with %d bytes of VRAM." % (self.__total_vram))

  def __calculate_resource_requirements(self, job):
    """ Calculates the percentage resource requirements for running a particular
    job.
    Returns:
      A tuple containing the requirements for CPU, RAM, GPU, and VRAM. """
    usage = job.get_resource_usage()

    # CPU requirements will be in percentages, so we just need to divide this by
    # the number of cores.
    cpu = usage.Cpu / self.__cpu_cores
    # RAM is in bytes, so we can just calculate the fraction.
    ram = usage.Ram / self.__total_ram * 100
    # GPU requirement will be in percentage form already.
    gpu = usage.Gpu
    # VRAM is in bytes, so we can calculate the fraction again.
    vram = usage.Vram / self.__total_vram

  def add_job(self, job_directory):
    """ Adds a new job to the queue.
    Args:
      job_directory: The directory containing the job. """
    logger.info("Adding new job from '%s'." % (job_directory))

    try:
      new_job = job.Job(job_directory)
    except job.ConfigurationError as error:
      # Bad configuration. Don't add the job.
      logger.error("Failed to add job: %s" % str(error))
      return

    self.__pending_jobs.appendleft(new_job)

    # update 

  def update(self):
    """ Updates the state of the manager. Should be called periodically. """
    # Remove any jobs that are now finished.
    for job in self.__running_jobs:
      if job.is_finished():
        logger.debug("Removing completed job: %s" % (job.get_name()))
        self.__running_jobs.remove(job)
