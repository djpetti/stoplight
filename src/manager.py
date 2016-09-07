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
  def __init__(self):
    # This is the queue that keeps track of our pending jobs.
    self.__pending_jobs = deque()
    # This is the set that keeps track of our running jobs.
    self.__running_jobs = set()
    # Jobs that might currently be runnable.
    self.__maybe_runnable = deque()
    # Jobs that were started from maybe_runnable. This is so we can easily
    # remove them from pending_jobs the next time we go through the queue.
    self.__already_started = set()

    # TODO(danielp): Support for multiple GPUs.
    self.__gpu = nvidia.Gpu(0)

    # Current usage percentages for each of the resources.
    self.__cpu_usage = 0
    self.__ram_usage = 0
    self.__gpu_usage = 0
    self.__vram_usage = 0

    self.__get_available_resources()

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

    return cpu, ram, gpu, vram

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

    # We haven't checked whether this job is runnable yet.
    self.__maybe_runnable.appendleft(new_job)

  def update(self):
    """ Updates the state of the manager. Should be called periodically. """
    # Remove any jobs that are now finished.
    finished_job = False
    to_remove = []
    for job in self.__running_jobs:
      if job.is_finished():
        to_remove.append(job)
        finished_job = True

        # Reclaim the resources used by the job.
        cpu, ram, gpu, vram = self.__calculate_resource_requirements(job)
        self.__cpu_usage -= cpu
        self.__ram_usage -= ram
        self.__gpu_usage -= gpu
        self.__vram_usage -= vram

    # We can't remove jobs from the set we're iterating through, so...
    for job in to_remove:
      logger.debug("Removing completed job: %s" % (job.get_name()))
      self.__running_jobs.remove(job)

    # Check to see if there are any new jobs that we can start running.
    cpu_remaining = 100 - self.__cpu_usage
    ram_remaining = 100 - self.__ram_usage
    gpu_remaining = 100 - self.__gpu_usage
    vram_remaining = 100 - self.__vram_usage

    pending_queue = self.__pending_jobs
    if not finished_job:
      # If no jobs finished this cycle, then our resource usage profile didn't
      # change, so we only have to look at new jobs that were added.
      logger.debug("No jobs finished, using maybe_runnable queue.")
      pending_queue = self.__maybe_runnable

    # If we just finished a job, our current load changed, so we have to go
    # through and check the entire pending queue.
    seen_jobs = 0
    while (cpu_remaining > 0 and ram_remaining > 0 and \
           gpu_remaining > 0 and vram_remaining > 0):
      if not len(pending_queue):
        # We have no work to do.
        logger.debug("No more jobs to run.")
        break
      if seen_jobs >= len(pending_queue):
        # We have more jobs to run, but insufficient resources to run them.
        logger.debug("Not enough resources to run more jobs.")
        break;

      # Get the next job to check.
      job = pending_queue.pop()
      if job in self.__already_started:
        # We already started this job on a pass through maybe_runnable, so we
        # can ignore it this time.
        self.__already_started.remove(job)
        continue

      cpu, ram, gpu, vram = self.__calculate_resource_requirements(job)

      if (cpu <= cpu_remaining and ram <= ram_remaining and \
          gpu <= gpu_remaining and vram <= ram_remaining):
        # Start the job.
        logger.info("Starting new job: %s" % (job.get_name()))
        job.start()

        cpu_remaining -= cpu
        ram_remaining -= ram
        gpu_remaining -= gpu
        vram_remaining -= vram

        self.__running_jobs.add(job)

        if pending_queue == self.__maybe_runnable:
          # Indicate that we've already started this job, so we should ignore
          # the copy in pending_jobs.
          self.__already_started.add(job)

      else:
        # If we don't have enough resources, add it back to the queue.
        pending_queue.appendleft(job)

      seen_jobs += 1

    # Since everything in this queue is also in the pending queue, we've processed
    # it already.
    self.__maybe_runnable.clear()

    # Update resource usage.
    self.__cpu_usage = 100 - cpu_remaining
    self.__ram_usage = 100 - ram_remaining
    self.__gpu_usage = 100 - gpu_remaining
    self.__vram_usage = 100 - vram_remaining

    logger.debug("Resource usage: CPU: %d, RAM: %d, GPU: %d, VRAM: %d" % \
                 (self.__cpu_usage, self.__ram_usage, self.__gpu_usage,
                  self.__vram_usage))

