import random
from datetime import timedelta
import numpy as np

def generate_random_datetime(start, end):
    delta = end - start
    random_seconds = random.uniform(0, delta.total_seconds())
    return start + timedelta(seconds=random_seconds)

def randomly_pick_from_weighted_map(keys, weights_dict):
    weight_list = [weights_dict[x]["weight"] for x in keys]
    return random.choices(population=keys, weights=weight_list, k=1)[0]

def randomly_pick_from_weighed_list(keys, weight_list):
    return random.choices(population=keys, weights=weight_list, k=1)[0]

def sample_lognormal(mu, sigma):
    return np.random.lognormal(mean=mu, sigma=sigma)
