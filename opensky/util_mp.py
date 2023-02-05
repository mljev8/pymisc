
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# GENERIC STUFF

def dict_io_mp(a: dict, task, n_jobs: int = 4):
    """ 
    multiprocessing standard use case
    'a' contains data to be processed individually by 'task', ordering irrelevant
    results returned as dictionary 'b'
    """
    b = {} # output dict

    n_threads = mp.cpu_count() - 2 # upper limit, somewhat safe
    n_jobs_safe = min(n_jobs, n_threads)
    if (n_jobs_safe < n_jobs):
        print(f'INFO: n_jobs reduced to {n_jobs_safe} in dict_io_mp()')
    
    n = len(a)
    with mp.Pool(n_jobs_safe) as pool, tqdm(total=n) as pbar:
        for key,result in pool.imap_unordered(task, a.items()):
            b[key] = result
            pbar.update()

    return b
#

def dict_inplace_io_tpe(a: dict, task_inplace, b: dict, n_jobs: int = 4):
    """ 
    ThreadPoolExecutor standard use case
    'a' contains data to be processed individually by 'task_inplace', no ordering
    results delivered inplace within forwarded dictionary 'b'
    signature: def task_inplace(item_from_a, dict_b) -> None
    """

    n_threads = mp.cpu_count() - 2 # upper limit, somewhat safe
    n_jobs_safe = min(n_jobs, n_threads)
    if (n_jobs_safe < n_jobs):
        print(f'INFO: n_jobs reduced to {n_jobs_safe} in dict_inplace_io_tpe()')
    
    n = len(a)
    with ThreadPoolExecutor(n_jobs_safe) as tpe, tqdm(total=n) as pbar:
        futures = [tpe.submit(task_inplace, item, b) for item in a.items()]
        for f in as_completed(futures):
            pbar.update()
    
    return # no return arg, this is an inplace convenience routine
#
