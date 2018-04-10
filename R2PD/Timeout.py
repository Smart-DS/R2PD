import signal


class TimeoutError(Exception):
    """
    Custom Error for Timeout
    """
    pass


class Timeout(object):
    """
    Timeout wrapper for signal.alarm
    Currently only compatible with linux systems, need to update for windows
    """
    def __init__(self, sec):
        """
        Initialize timeout

        Parameters
        ----------
        sec : 'float'
            seconds before raising TimeoutError
        """
        self.sec = sec

    def __enter__(self):
        """
        Enter method to allow use of with
        """
        signal.signal(signal.SIGALRM, self.raise_timeout)
        signal.alarm(self.sec)

    def __exit__(self, type, value, traceback):
        """
        Closes event_loop on exit from with

        Parameters
        ----------
        type : 'Error'
            error type
        value : 'str'
            error message
        traceback : 'str'
            error traceback
        """
        # Reset alarm
        signal.alarm(0)

        if type is not None:
            raise

    def raise_timeout(self, *args):
        """
        Closes event_loop on exit from with

        Parameters
        ----------
        *args : 'signal.signal handler args'
            signal number and frame
        """
        raise TimeoutError("Connection timed out after {:} seconds!"
                           .format(self.sec))
