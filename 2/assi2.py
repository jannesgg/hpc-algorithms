"""
Compute statistics on data

"""

from mrjob.job import MRJob

class WC(MRJob):
    
    def mapper(self, _, line):        
        value = float(line.split()[-1])
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
            yield('std', std)
        
        if key == 'min':
            yield('min', min(stat))
        
        if key == 'max':
            yield('max', max(stat))

        if key == 'bin0':
            yield ('bin0', sum(stat))
        if key == 'bin1':
            yield ('bin1', sum(stat))
        if key == 'bin2':
            yield ('bin2', sum(stat))        
        if key == 'bin3':
            yield ('bin3', sum(stat))
        if key == 'bin4':
            yield ('bin4', sum(stat))            
        if key == 'bin5':
            yield ('bin5', sum(stat))
        if key == 'bin6':
            yield ('bin6', sum(stat))      
    
if __name__ == '__main__':
    WC.run()
            
            