"""
Compute statistics on data

"""

from mrjob.job import MRJob, MRStep

class WC(MRJob):
    
    def configure_args(self):
        super(WC, self).configure_args()
        self.add_passthru_arg('--groups',
                       default=[],
                        nargs="*",
                       type = int,
                       help = 'A list, specifying groups of data used to compute summary statistics' )
    def mapper_init(self):
        self.groups = set(self.options.groups)
    
    def mapper(self, _, line):        
        group, value = line.split()[-2:]
        group = int(group)
        value = float(value)
        if self.groups == set([]) or group in self.groups:
            yield ('sum', value)
            yield ('min', value)
            yield ('max', value)
        
            if value<0:
                yield('bin0', 1)
            if value>=0 and value<0.2:
                yield('bin1', 1)
            if value>=0.2 and value<0.4:
                yield('bin2', 1)
            if value>=0.4 and value<0.6:
                yield('bin3', 1)
            if value>=0.6 and value<0.8:
                yield('bin4', 1)
            if value>=0.8 and value<=1:
                yield('bin5', 1)
            if value>1:
                yield('bin6', 1)

    
    def combiner(self, key, values):
        
        if key == 'sum':
            total = 0
            total_square = 0
            for i,value in enumerate(values):
                total += value
                total_square += value**2
            #sum, sum of squares, count
            yield ('sums', (total, total_square, i+1))
        if key == 'min':
            yield ('min', min(values))
        if key == 'max':
            yield ('max', max(values))
            
        if key == 'bin0':
            yield ('bin0', sum(values))
        if key == 'bin1':
            yield ('bin1', sum(values))
        if key == 'bin2':
            yield ('bin2', sum(values))        
        if key == 'bin3':
            yield ('bin3', sum(values))
        if key == 'bin4':
            yield ('bin4', sum(values))            
        if key == 'bin5':
            yield ('bin5', sum(values))
        if key == 'bin6':
            yield ('bin6', sum(values))            
            
            
    def reducer(self, key, stat):
        
        if key == 'sums':
            total = 0
            total_square = 0
            n = 0
            for i, tup in enumerate(stat):
                total += tup[0]
                total_square += tup[1]
                n += tup[2]
            mean = float(total/n)
            var = total_square/(n-1) - mean**2 * n/(n-1)
            std = var**0.5
            yield('mean, std', (n, mean, std))
        
        if key == 'min':
            yield('min', min(stat))
        
        if key == 'max':
            yield('max', max(stat))
            
        else:
            yield ('median', (key, sum(stat)))
        
            
    def compute_median(self, key, computed):
        
        if key == "min":
            yield("min", computed)
        if key == "max":
            yield("max", computed)
        if key == "mean, std":
            yield("count, mean, std", computed)
   
        if key == 'median':
            a = {}
            for tup in computed:
                a[tup[0]] = tup[1]
            count = 0
            total = sum(a.values())
            for i in range(5):
                key = 'bin{:s}'.format(str(i+1))
                count += a[key]
                if count >= total // 2:
                    yield('median_bin', (i*0.2, (i+1)*0.2))
                    yield('median approximation', 0.2*i + 0.2*(((total // 2)-(count-a[key]))/(a[key])))
                    break
                
    def steps(self):
        return [MRStep(mapper_init=self.mapper_init,mapper=self.mapper,
                      combiner=self.combiner,
                      reducer=self.reducer),
               MRStep(reducer=self.compute_median)]
        
    
if __name__ == '__main__':
    WC.run()
            
            