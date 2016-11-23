from multiprocessing import Array, Value, Lock
import pickle


class Mailbox:
  """
    A lock-protected bit of shared state that can be used to send a single
    value between processes. Thanks to Pickle, it can store pretty much
    anything.
  """

  # The size of the array that stores data.
  _SIZE = 1024

  def __init__(self):
    # Where we write the element.
    self.__state = Array('c', self._SIZE, lock=False)
    # Whether the slot is in use. If it's not, it's zero, otherwise it's the
    # length of whatever's in it.
    self.__used = Value('i', 0, lock=False)
    # Allows us to block until it's read.
    self.__read_lock = Lock()
    # Atomizes box operations.
    self.__sync_lock = Lock()

  def __clear_box(self):
    """ Clears all data stored in the box. """
    if not self.__used.value:
      # It's already clear.
      return

    self.__used.value = 0
    self.__read_lock.release()

  def set(self, obj):
    """ Puts new data in the box.
    Args:
      obj: What to put in the box. """
    self.__sync_lock.acquire()

    if self.__used.value:
      self.__clear_box()

    # Pickle the data and write it in.
    pickled = pickle.dumps(obj)
    if len(pickled) > self._SIZE:
      raise ValueError("Object of size %d exceeds maximum size of %d!" % \
                       (len(pickled), self._SIZE))

    self.__state.raw = pickled
    self.__used.value = len(pickled)

    self.__sync_lock.release()
    self.__read_lock.acquire()

  def __do_read(self):
    """ Gets whatever's in the box, but does not clear it.
    It does not do any locking.
    Returns:
      The data, or None if the box is empty. """
    if not self.__used.value:
      return None

    # Get the pickled item.
    pickled = self.__state.raw
    pickled = pickled[:self.__used.value]

    return pickle.loads(pickled)

  def peek(self):
    """ Gets whatever's in the box, but does not clear it. If something is
    waiting on reads, this does not count as one. Only get() does.
    Returns:
      The data, or None if the box is empty. """
    self.__sync_lock.acquire()
    loaded = self.__do_read()
    self.__sync_lock.release()

    return loaded

  def get(self):
    """ Gets whatever's in the box.
    Returns:
      The data, or None if the box is empty. """
    self.__sync_lock.acquire()

    loaded = self.__do_read()
    self.__clear_box()

    self.__sync_lock.release()
    return loaded

  def wait_for_read(self):
    """ Blocks until the value in the box is read. If the box is empty, it
    returns immediately. """
    self.__read_lock.acquire()
    self.__read_lock.release()

