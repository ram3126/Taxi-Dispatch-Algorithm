import pandas as pd
from sklearn.neighbors import NearestNeighbors
from sklearn.metrics.pairwise import haversine_distances
from math import radians
from sklearn.metrics import DistanceMetric
import numpy as np


def find_nearest_drivers_within_radius(ride_lat,ride_long,radius,drivers):

    ######### Find the distance for driver ######################
    identifiers = []
    coordinates = []
    for obj in drivers:
        identifiers.append(obj['id'])
        coordinates.append(obj['current_location'])

    # Convert the coordinates to radians
    coordinates_rad = np.radians(coordinates)

    # Create the distance metric
    dist = DistanceMetric.get_metric('haversine')

    # Find the objects within the radius
    neighbors = []
    for i, coord in enumerate(coordinates_rad):
        distance = dist.pairwise([np.radians([ride_lat, ride_long]), coord])[0][1] * 6371.0
        if distance <= radius:
            neighbors.append(drivers[i])

    return neighbors



    # Print the nearby objects
    # for obj in neighbors:
    #     print(obj['id'], obj['price'], 'is within', radius, 'km radius')



###################### Testing Area #######################################################################
# drivers = [{'id':1,'current_location': (40.89916730614381, -73.89380879452553), 'price': 21}, {'id':2,'current_location': (40.529850145838935, -73.9931805918343), 'price': 25}, {'id':3,'current_location': (40.76394222253736, -74.18843915275276), 'price': 21}, {'id':4,'current_location': (40.6045561255853, -73.94326879750957), 'price': 23}]
# neighbors = find_nearest_drivers_within_radius(40.75,-73.76,100,drivers)
# print(neighbors)
###########################################################################################################