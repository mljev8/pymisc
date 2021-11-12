import numpy as np
#from math import pi,cos,sin,acos,atan2

deg2rad = np.pi/180.
rad2deg = 180./np.pi

def spherical2cartesian(rho, phi, theta):
    """ 
    Mathematical convention (phi = elevation [rad], theta = azimuth [rad]).
    https://en.wikipedia.org/wiki/Spherical_coordinate_system
    """
    x = rho * np.sin(phi) * np.cos(theta)
    y = rho * np.sin(phi) * np.sin(theta)
    z = rho * np.cos(phi)
    return x,y,z

def cartesian2spherical(x, y, z):
    rho = np.sqrt(x*x + y*y + z*z)
    phi = np.arccos(z/rho)
    theta = np.arctan2(y, x) # np.arctan(y/x)
    return rho,phi,theta

def latlonalt2cartesian(lat, lon, alt):
    """
    Northern latitude [deg], Eastern longitude [deg], altitude [feet].
    https://en.wikipedia.org/wiki/World_Geodetic_System
    Not exact, but for most purposes good enough.
    """    
    rho = 6371000. + 0.3048*alt # [m]
    phi = 0.5*np.pi - deg2rad * lat
    theta = deg2rad * lon
    return spherical2cartesian(rho,phi,theta)
    
def cartesian2latlonalt(x, y, z):
    rho,phi,theta = cartesian2spherical(x, y, z)
    lat = 90. - rad2deg * phi
    lon = rad2deg * theta
    alt = (rho - 6371000.)/0.3048 # [feet]
    return lat,lon,alt

