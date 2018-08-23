from mrjob.job import MRJob, MRStep
import sys

class Sum_stats(MRJob):
    
    def mapper(self, _, line):
        value = float(line.split('\t')[2])
        yield ("sum", value)
        
    def combiner(self, key, counts):
        if key == "sum":
            total = 0
            squares = 0
            minimum = sys.float_info.max
            maximum = -minimum
            for i,c in enumerate(counts):
                total += c
                squares += c**2
                if c < minimum: 
                    minimum = c
                if c > maximum:
                    maximum = c
            yield("stats",(total, i+1, squares, minimum, maximum))
        
    def reducer(self, key, counts):
        total_values = 0
        total_num = 0
        total_val_sq = 0
        for c in counts:
            total_values += c[0]
            total_val_sq += c[2]
            total_num += c[1]
            yield ("min", c[3])
            yield ("max", c[4])
            
        
        yield ("stats", (float(total_values/total_num),
               ((1/(total_num-1))*(float(total_val_sq)-total_num*float(total_values/total_num)**2))**0.5))
        
    def find_stats(self, key, counted):
        if key=="min":
            yield ("min", min(counted))
        elif key=="max":
            yield ("max", max(counted))
        else:
            yield ("stats", counted)
        
    def steps(self):
        return [MRStep(mapper=self.mapper,
                      combiner=self.combiner,
                      reducer=self.reducer),
               MRStep(reducer=self.find_stats)
               ]
        
            
if __name__ == '__main__':     
    Sum_stats.run()