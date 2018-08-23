""" Find duplicate keys in a file containing lines consisting of
    key,value 
"""
from mrjob.job import MRJob, MRStep

class FindDuplicates(MRJob):

    def mapper(self, _, line):
        key, value = line.split(',')
        yield (key, 1)

    def combiner(self, key, counts):
        yield (key, sum(counts))

    def reducer(self, key, counts):
        s = sum(counts)
        yield None, (s, key)

    def findmax(self, _, count_word_tuples):
        yield max(count_word_tuples)

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer),
            MRStep(reducer=self.findmax)]

if __name__ == '__main__':
     FindDuplicates.run()
