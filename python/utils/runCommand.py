import logging
import subprocess as sub


def run_cmd(cmd):
    try:
        out = sub.check_output(cmd, shell=True, stderr=sub.STDOUT)
        return out
    except sub.CalledProcessError as err:
        logging.error("The failed test setup command was [%s]." % err.cmd)
        logging.error("The output of the command was [%s]" % err.output)
        raise
