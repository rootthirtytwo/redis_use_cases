import yaml
import numpy as np
import random

def yaml_loader(file_path):
    '''To load file to variable'''
    with open(file_path, "r") as f:
        data = yaml.load(f)
    return data

def get_key_name(*args):
    return str(":".join(args))


def get_order_id():
    return random.randint(100000, 999999)
