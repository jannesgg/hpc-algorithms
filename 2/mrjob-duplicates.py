""" Find duplicate keys in a file containing lines consisting of
    key,value 

    Run with python mrjob-duplicates.py -r local test-big.data
"""
from mrjob.job import MRJob

class FindDuplicates(MRJob):

    def mapper(self, _, line):
        key, value = line.split(',')
        yield (key, 1)

    def reducer(self, key, counts):
        s = sum(counts)
        if s > 1:
            yield (key, s)


if __name__ == '__main__':
     FindDuplicates.run()
