import logging
import subprocess


logger = logging.getLogger()


class ConfigurationError(Exception):
  """ Custom exception for malformed config files. """
  pass


def get_path(tool):
    """ Gets the path of a particular tool.
    Args:
      tool: The tool to find the path of.
    Returns:
      The path to the tool. """
    try:
      path = subprocess.check_output(["/usr/bin/which", tool])
    except subprocess.CalledProcessError:
      logger.critical("You should have installed '%s' beforehand." \
                      " Your mother and I are very disappointed in you." % (tool))
      sys.exit(1)

    return path.decode("utf-8").rstrip("\n")

