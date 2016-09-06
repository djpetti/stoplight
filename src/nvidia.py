import subprocess
import xml.etree.ElementTree as et

import util


""" Gets information about an NVIDIA GPU. """


class Gpu:
  """ Represents a single GPU in the system. """

  def __init__(self, gpu_id):
    """
    Args:
      gpu_id: The numerical ID of the GPU, as listed by nvidia-smi -L. """
    self.__gpu_id = gpu_id

    self.__nvidia_smi = util.get_path("nvidia-smi")

    self.__gather_global_data()

  def __gather_global_data(self):
    """ Gathers data about a GPU which doesn't change. """
    # Find the total amount of VRAM.
    command = [self.__nvidia_smi, "-i", str(self.__gpu_id), "-q", "-x"]
    output = subprocess.check_output(command)
    # Parse XML output.
    tree = et.fromstring(output)

    gpu = tree.find("gpu")
    memory = gpu.find("fb_memory_usage")
    vram = memory.find("total").text
    # Convert to bytes.
    self.__total_vram = int(vram.split()[0]) * 1000000

  def get_total_vram(self):
    """
    Returns:
      The total amount of VRAM that the system has. """
    return self.__total_vram
