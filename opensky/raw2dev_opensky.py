"""
Convert daily .h5 files containing hourly .gz.csv files into new daily .h5 files
containing per-aircraft data (sort by [hex,time] and then slice by hex).

Logic:
- Loop over daily .h5 files
- Read gz bytes into dict (n=24)
- Use MP helper to apply gzip.decompress() and numpy.genfromtxt() (MEMORY ABUSE)
- Sort by hex and time, group-by wrt hex
- Dump to daily .h5 on disk, indexed by hex
"""

import io
import pathlib
import h5py
import yaml
import gzip
import numpy as np
from tqdm import tqdm
from datetime import datetime

with open('config-opensky.yaml', 'r') as f:
    config = yaml.safe_load(f)

input_glob_pattern = config['input-glob-pattern']
input_dir = pathlib.Path(config['input-dir']) 
output_dir = pathlib.Path(config['output-dir'])
output_dir.mkdir(exist_ok=True)

opensky_states_dtype = np.dtype(config['opensky-states-dtype']).descr
omit_fields_list = config['omit-fields-list']

opensky_slim_dtype = [a for a in opensky_states_dtype if a[0] not in omit_fields_list]
usecols = [i for i,a in enumerate(opensky_states_dtype) if a[0] not in omit_fields_list]
kwargs = dict(delimiter=',', skip_header=1, dtype=opensky_slim_dtype, usecols=usecols)

# main
for hdf5_file in sorted(input_dir.glob(input_glob_pattern))[-3:]:
    print(f'\nDecompressing and parsing hourly files ({hdf5_file.stem}):')
    A = np.zeros(shape=(0,), dtype=opensky_slim_dtype)
    with h5py.File(hdf5_file, 'r') as infile:
        for key,gz_bytes in tqdm( infile.items()):
            bytes_io = io.BytesIO( gzip.decompress(gz_bytes) )
            npsa = np.genfromtxt(bytes_io, **kwargs)
            A = np.concatenate([A,npsa])
    # merge, sort by hex and time, group-by, dump to disk
    omit = np.isnan(A['time'][:] + A['lat'][:] + A['lon'][:] + A['baroaltitude'][:])
    print(f"\nn={len(A)} entries in total of which {omit.sum()} contain NaN's ({100.*omit.sum()/len(A):0.1f}%)")
    A = A[~omit] # not omit = keep
    n = len(A)
    print(f'Sorting... (n={n})')
    A[:] = A[np.argsort(A[['icao24','time']])] # sort by hex, and then by time
    hex_diff = (A['icao24'][1:] != A['icao24'][0:-1]) # np.diff
    edges = np.arange(n+1)[np.r_[True,hex_diff,True]] # slicing boundaries 
    n_hex = len(edges) - 1
    print('Dumping data to .h5, looping through all aircrafts, one after another:')
    outname = f'{hdf5_file.name[0:8]}_opensky_per_aircraft.h5'
    with h5py.File(output_dir / outname, 'w') as outfile:        
        for i,j in tqdm(zip(edges[:-1],edges[1:]),total=n_hex): # lhs,rhs                
            B = A[i:j] # data for one single hex, i.e. single aircraft
            hex_ = B['icao24'][0]
            datetime_emerge = datetime.utcfromtimestamp(B['time'][0])                
            yyyymmdd,hhmmss = datetime_emerge.strftime('%Y%m%d,%H%M%S').split(',')
            key = f'{yyyymmdd}-{hhmmss}-{hex_.decode()}'
            outfile.create_dataset(key, data=B)
    del A,B # attempt to clean up
