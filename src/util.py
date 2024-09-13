import os

def leave_container():
    """
    Set file permissions of /out (from docker user) and delete unused tool-runner files.

    """
    os.system("chmod -R 777 /out/hyras")
    os.system("rm /out/errors.log")
    os.system("rm /out/processing.log")