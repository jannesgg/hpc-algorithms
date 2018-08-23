import multiprocessing as mp
from queue import Empty 
import math
import os
import time
import argparse
import random
from math import pi
import numpy as np


def sample_pi(n):
    """ Perform n steps of Monte Carlo simulation for estimating Pi/4.
        Returns the number of sucesses."""
    s = 0
    for i in range(n):
        x = random.random()
        y = random.random()
        if x**2 + y**2 <= 1.0:
            s += 1
    a = s / n 
    return s

def worker(in_q, out_q):
    while True:
        if not in_q.empty():
            x = in_q.get()
            if x == 0:
                print("Worker is done")
                #in_q.task_done()
                break
            else:
                print("Worker is working...")
                out_q.put(sample_pi(x))
        else:
            time.sleep(0.1)
            print("Worker idle")
        
        
def compute_pi(args):
    
    in_q = mp.Queue()
    out_q = mp.Queue()
    
    n_total = 0
    s_total = 0
    
    r = args.workers*[100]
    for i in r:
        in_q.put(i)
    n_total += sum(r)
    
    jobs = []
    for i in range(args.workers):
        proc = mp.Process(target=worker, args=(in_q, out_q))
        proc.start()
        jobs.append(proc)
    print("We have our workers in line! Yay for us! 2018 is our year!")
    
    while True: 
        time.sleep(0.1)
        if not out_q.empty():
            o = [out_q.get() for i in range(args.workers)]
            s_total += sum(o)
            #print(s_total, n_total)
            pi_est = 4 * (s_total/n_total)
            acc = np.abs(pi_est-pi)
            print(acc)
            if acc > args.accuracy:
                for i in args.workers*[100]:
                    in_q.put(i)
                n_total += sum(args.workers*[100])
            else:
                for i in range(args.workers):
                    in_q.put(0)
                print("We're all in this together!")
                print (" Steps\tSuccess\tPi est.\tError")
                print ("%6d\t%7d\t%1.5f\t%1.5f" % (n_total, s_total, pi_est, pi-pi_est))
                break

    for w in jobs:
        w.join()

    print("We done joining people!")


    

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
    

    
    start = time.time()
    compute_pi(args)
    end = time.time()
    print('Algorithm running time {:.2f} secs. '.format(end-start))