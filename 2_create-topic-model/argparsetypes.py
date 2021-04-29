import argparse
import os


def dir_path_type(path: str):
    if os.path.isdir(path):
        return path
    else:
        raise argparse.ArgumentTypeError(f"Folder {path} does not exist")


def num_cores_type(cores: str) -> int:
    if cores.isdigit() and int(cores) > 0:
        return int(cores)
    else:
        raise argparse.ArgumentTypeError(f"Invalid number of cores specified: {cores}")
