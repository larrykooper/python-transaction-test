import inspect
import os

def rel_path(relative_filename):
    """Returns the full path of the file relative to the caller module.
    :param relative_filename: target filename relative to the caller's
        containing folder.
    :return: the full path of the target relative file.
    """
    # Extract the filename of the caller's stack frame.
    caller_frame = inspect.stack()[1]
    try:
        caller_filepath = inspect.getabsfile(caller_frame[0])
    finally:
        # Force remove frame reference to prevent garbage-collection issues.
        del caller_frame

    return os.path.join(
        os.path.dirname(os.path.realpath(caller_filepath)),
        relative_filename)