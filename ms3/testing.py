""" Helper module for using MS3 in tests """
import os
import time
import signal
import urllib


def wait_until(func, *args):
    t1 = time.time()
    while not func(*args):
        time.sleep(0.1)
        if time.time() - t1 > 3: # seconds
            raise Exception("wait_until %s timeout raised", func)


def is_running(port):
    try:
        urllib.urlopen("http://localhost:%d" % port)
    except IOError:
        return False
    return True


class MS3Server(object):
    """ Class for managing a ms3 server """
    _pid = None
    _port = None
    datadir = None


    @classmethod
    def start(cls, datadir=None, config=None, port=9010, with_exec=False):
        """
            Start the MS3 server with the provided data directory. This method
            will fork the process and start a server in the child process.

            with_exec specifies if the server should overwrite the child
            process image (with it's own environment and runnable code). Use
            this when in the project you're testing you're also using tornado
            (maybe a different version of tornado).
        """
        assert not cls._pid
        cls._port = port
        cls.datadir = datadir
        cls._pid = os.fork()
        if cls._pid == 0:
            args = []  # "--debug=True", "--logging=debug"]
            if datadir:
                args.append("--datadir=%s" % datadir)
            if config:
                args.append("--config=%s" % config)
            if port:
                args.append("--port=%s" % port)
            if with_exec:
                ms3_base_path = os.path.normpath(os.path.join(os.path.dirname(
                    os.path.abspath(__file__)), '..'))
                ms3_bin_path = os.path.join(ms3_base_path, "bin")
                args = [os.path.join(ms3_bin_path, "python"), "-m",
                        "ms3.testing"] + args
                env = {'PATH': ":".join([ms3_bin_path, ms3_base_path,
                                         os.getenv('PATH')]),
                       'VIRTUAL_ENV': ms3_base_path,
                       'PYTHONPATH': ms3_base_path,
                       'PWD': ms3_base_path}
                os.execve(args[0], args, env)
            else:
                import ms3.app
                args.insert(0, None)  # pass tornado options
                ms3.app.run(args)
        else:
            wait_until(lambda: not is_running(cls._port))

    @classmethod
    def stop(cls):
        """ Stop a started MS3 Server """
        if cls._pid:
            os.kill(cls._pid, signal.SIGTERM)
            cls._pid = None
            cls.datadir = None
            wait_until(lambda: not is_running(cls._port))
            cls._port = None


if __name__ == "__main__":
    import ms3.app
    ms3.app.run()
