import math
### together with computation of mean, std, min and max
### bin count with reduceByKey
# global variables

nr_bins = 10
v_min = 0.0
v_max = 1.0
binwidth = (v_max - v_min) / nr_bins
hist_bins = {}

def read_data(line):
    value = float(line.split()[-1])
    return value

def distribute_data(value):
    ### wrong calculation of the bin if have value<min or >max
    if value < v_min:
        bin_nr = 0
    elif value > v_max:
        bin_nr = nr_bins - 1
    else:
        bin_nr = int((value - v_min) / binwidth)
    return (bin_nr, (1,value,value**2, value,value))

def summary_stats(value1, value2):
    bin_min = min(value1[3], value2[3])
    bin_max = max(value1[4], value2[4])
    bin_sum = value1[1]+value2[1]
    bin_sos = value1[2] + value2[2]
    bin_count = value1[0] + value2[0]
    
    return (bin_count, bin_sum,bin_sos,bin_min,bin_max)
    

def output(bin_sum):
    """
    bin_sum: summary stats of each bin
    return: summary stats histogram info of the data
    """
    histogram = {}
    n_total = 0
    sum_total = 0
    sos_total = 0 #total sum of squares
    min_list = []
    max_list = []
    
    for i in range(len(bin_sum)):
        histogram[bin_sum[i][0]] = bin_sum[i][1][0]
        n_total += bin_sum[i][1][0]
        sum_total += bin_sum[i][1][1]
        sos_total += bin_sum[i][1][2]
        min_list.append(bin_sum[i][1][3])
        max_list.append(bin_sum[i][1][4])
    
    total_mean = sum_total / n_total    
    total_var = sos_total/(n_total-1) - total_mean*total_mean*n_total/(n_total-1)
    total_std = math.sqrt(total_var)
    total_min = min(min_list)
    total_max = max(max_list)
    return histogram, {'Total Mean': total_mean, 'Total StdDev': total_std,
                       'Overall Min': total_min, 'Overall Max': total_max}


if __name__ == '__main__':

    from pyspark import SparkContext

    #SparkContext
    try:
        sc.stop()
    except:
        pass
    sc = SparkContext(master = 'local[4]')

    #Get data
    data = sc.textFile('test.data')

    values = data.map(read_data)
    binNr_v_pairs = values.map(distribute_data)
    bin_counts = binNr_v_pairs.reduceByKey(summary_stats)\
                .sortByKey()
    bin_sum = bin_counts.collect()
    print(output(bin_sum))