from mrjob.job import MRJob
from mrjob.step import MRStep
import mrjob
import pdb
import os

class MRFilter(MRJob):

    def configure_options(self):
        super(MRFilter, self).configure_options()
        self.add_passthrough_option('--A', help="Matrix A")
        self.add_passthrough_option('--B', help="Matrix B")

    def steps(self):
        return [
            MRStep(mapper=self.mapper1,
                   reducer=self.reducer1),
            MRStep(mapper=self.mapper2,
                   reducer=self.reducer2)]

    def mapper1(self, _, line):
        # Each line is CSV row vector of matrix
        a,b,v = line.split(',')
        a = int(a)
        b = int(b)
        v = float(v)

        filename = os.environ['map_input_file']
        if filename == self.options.A:
                yield b,(0,a,v)
        elif filename == self.options.B:
                yield a,(1,b,v)

    def reducer1(self, k, tuples):
        t = list(tuples)
        ta = [x[1:] for x in t if x[0] == 0]
        tb = [x[1:] for x in t if x[0] == 1]
        r = [((x[0],y[0]),x[1]*y[1]) for x in ta for y in tb]
        for e in r:
            yield e[0], e[1]

    def mapper2(self, pik, product):
        yield pik, product

    def reducer2(self, pik, product):
        yield pik, sum(product)

if __name__ == '__main__':
    MRFilter.run()
