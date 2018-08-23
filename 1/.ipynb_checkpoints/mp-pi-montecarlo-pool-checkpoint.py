import multiprocessing
import argparse # See https://docs.python.org/2/howto/argparse.html for a tutorial
import random
import numpy as np
from math import pi
import time

def sample_pi(q, n):
    """ Perform n steps of Monte Carlo simulation for estimating Pi/4.
        Returns the number of sucesses."""
    #print ("Hello from a worker")
    random.seed()
    s = 0
    for i in range(n):
        n += 1
        x = random.random()
        y = random.random()
        if x**2 + y**2 <= 1.0:
            s += 1
        q.put(s)
    return s

def compute_pi(args):
    jobs = []
    n = 1000
    output = multiprocessing.Queue()
    workers = [multiprocessing.Process(target=sample_pi, args=(output, n//args.workers)) for i in range(args.workers)]
    jobs.append(workers)
    
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    
    s = 0
    out = [output.get() for p in workers]

    s += sum(o[0] for o in out)

    pi_est = (4.0*s_total)/n
    
    print("Accuracy: " + str(1-np.abs(pi-pi_est)/pi))
    print("Workers: " + str(args.workers))
    
    print ("Steps\tTrials\tSuccess\tPi est.\tError")
    print ("%6d\t%6d\t%7d\t%1.5f\t%1.5f" % (n_total/args.workers, n_total, s_total, pi_est, pi-pi_est))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Compute Pi using Monte Carlo simulation.')
    parser.add_argument('--workers', '-w',
                        default='1',
                        type = int,
                        help='Number of parallel processes')
    parser.add_argument('--accuracy', '-a',
                        default='0.98',
                        type = float,
                        help='Accuracy goal')
    args = parser.parse_args()
    s = time.time()
    compute_pi(args)
    e = time.time()
    print('Algorithm running time {:.2f} secs. '.format(e-s))
