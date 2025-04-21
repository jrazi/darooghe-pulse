import datetime
import os
import sys
from datetime import timedelta
from math import radians, sin, cos, sqrt, atan2
import pytest

from pulse.darooghe_pulse import generate_transaction_event

def haversine(a, b):
    earth_radius_km = 6371
    dlat = radians(b["lat"] - a["lat"])
    dlng = radians(b["lng"] - a["lng"])
    phi1 = radians(a["lat"])
    phi2 = radians(b["lat"])
    h = sin(dlat/2)**2 + cos(phi1)*cos(phi2)*sin(dlng/2)**2
    return 2 * earth_radius_km * atan2(sqrt(h), sqrt(1 - h))


def test_location_cluster():
    ts1 = datetime.datetime.utcnow()

    e1 = generate_transaction_event(timestamp_override=ts1)
    e2 = generate_transaction_event(timestamp_override=ts1 + timedelta(minutes=3))

    dist_km = haversine(e1["location"], e2["location"])
    assert dist_km < 5, f"distance too large: {dist_km}km"
