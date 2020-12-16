# -*- coding: utf-8 -*-
import subprocess

from pkg_resources import get_distribution, DistributionNotFound


def get_git_version():
    output = subprocess.check_output(('git', 'describe', '--tags'))
    return output.decode().strip()


try:
    # Change here if project is renamed and does not equal the package name
    dist_name = __name__
    __version__ = get_distribution(dist_name).version
except DistributionNotFound:
    __version__ = get_git_version()
finally:
    del get_distribution, DistributionNotFound
