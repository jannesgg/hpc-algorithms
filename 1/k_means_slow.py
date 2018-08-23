import numpy as np
import matplotlib.pyplot as plt
from sklearn.datasets import make_blobs
import time
import multiprocessing

def generateData(n, plot=None):
    X, y = make_blobs(n_samples=n, cluster_std=1.7, shuffle=False, random_state = 2122)
    if plot:
        fig, axes = plt.subplots(nrows=1, ncols=1)
        axes.scatter(X[:, 0], X[:, 1])
        axes.set_xticks([])
        axes.set_yticks([])
        plt.show()
    return X


def nearestCentroid(datum, centroids):
    # norm(a-b) is Euclidean distance, matrix - vector computes difference
    # for all rows of matrix
    dist = np.linalg.norm(centroids - datum, axis=1)
    return np.argmin(dist), np.min(dist)

def batch_centroid(ip, data, start, end, centroids, results_queue):
    c = np.zeros((end-start), dtype=int)
    cluster_sizes = np.zeros(centroids.shape[0], dtype=int)
    variation = 0.0
    for i in range(0,end-start):
        cluster, dist = nearestCentroid(data[i],centroids)
        c[i] = cluster
        cluster_sizes[cluster] += 1
        variation += dist**2
    print("Done with clustering!")
    results_queue.put([c, cluster_sizes, variation])
    return c, cluster_sizes

#### K-means
## 1. Choose k random data points (shared data)
## 2. For i in iterations:
##      calculate nearest centroid to allocate to cluster (can be parallelized)
## 3. Recompute centroids (Can be parallelized)


def kmeans(n_w, k, data, nr_iter = 100):
    
    N = len(data)

    # Choose k random data points as centroids
    np.random.seed(777)
    s1 = time.time()
    centroids = data[np.random.choice(np.array(range(N)),size=k,replace=False)]
    print ("Initial centroids\n", centroids)
    s2 = time.time()

    # The cluster index: c[i] = j indicates that i-th datum is in j-th cluster
    

    print ("Iteration\tVariation\tDelta Variation")
    total_variation = 0.0
    total_ass_time = 0
    total_recomp_time = 0
    delta_variation = 0.0
    
    for j in range(nr_iter):
        
        #print "=== Iteration %d ===" % (j+1)
        
        # Assign data points to nearest centroid
        s3 = time.time()
        
        breaks = list(range(0, N, N//n_w))
        breaks.append(N)
        ranges = [(breaks[i], breaks[i+1]) for i in range(0, len(breaks)-1)]
     
        results = multiprocessing.Queue()
        
        workers = [multiprocessing.Process(target=batch_centroid, args=(ip, data[r[0]:r[1]],r[0],r[1],centroids,results)) for
                   ip,r in enumerate(ranges)]
        
        for w in workers:
            w.start()
            
        for w in workers:
            w.join()
        
        
        output = [results.get() for w in range(len(workers))]
        #output = []
        
        #def yield_from_process(q, p):
            #while p.is_alive():
                #p.join(timeout=1)
                #while True:
                    #try:
                    #    output.append(q.get(block=False))
                    #except:
                    #    break
        
        #for w in workers:
         #   yield_from_process(results, w)

        total_cluster_size = sum(o[1] for o in output)
        delta_variation = -total_variation
        total_variation = sum(o[2] for o in output)
        delta_variation += total_variation
        
        stacked = [o[0] for o in output]
        c = np.array([item for o in stacked for item in o])
        print(c)
        
        print ("%3d\t\t%f\t%f" % (j, total_variation, delta_variation))
        s4 = time.time()
        res = s4-s3
        total_ass_time += res
        
        # Recompute centroids
        s5 = time.time()
        centroids = np.zeros((k,2))
        for i in range(N):
            centroids[c[i]] += data[i] 
        centroids = centroids / total_cluster_size.reshape(-1,1)
        print(centroids)
        s6 = time.time()
        res2 = s6-s5
        total_recomp_time += res2
        #print "Total variation", total_variation
        #print "Cluster sizes", cluster_sizes
        #print c
        #print centroids
    
    return s2-s1, total_variation, total_ass_time, total_recomp_time, c

if __name__ == "__main__":
    n_samples = 1000
    X = generateData(n_samples)
    start = time.time()
    init_time, total_var, total_ass, total_recomp, c = kmeans(2, 3, X, nr_iter = 20)
    end = time.time()
    print('Initial Step took: ' + str(init_time) + ' seconds')
    print('Assignment Step took: ' + str(total_ass) + ' seconds')
    print('Update Step took: ' + str(total_recomp) + ' seconds')
    print('Clustering took: ' + str(end-start) + ' seconds')

