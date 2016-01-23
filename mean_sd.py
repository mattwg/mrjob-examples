from mrjob.job import MRJob
import mrjob
import pdb

# No combiner!

class MRCount(MRJob):

    def mapper(self, _, line):
        # Each line is CSV
        # Skip header and emit month and count
        l = [s.strip('"') for s in line.split(',')]
        if l[0] != '' and l[7] != 'NA':
            # strip off quotes
            yield l[12], int(l[7])


    def reducer(self, origin, tuples):
        # Convert generator to list - multiple passes required
        t = list(tuples)
        t_sum = float(sum(t))
        t_n = len(t)
        t_mean = t_sum / t_n
        if t_n > 1:
            t_sd = ( sum( (x - t_mean) ** 2 for x in t ) / ( t_n - 1 ) ) ** 0.5
        else:
            t_sd = None
        yield origin, ( t_mean, t_sd )

if __name__ == '__main__':
    MRCount.run()
