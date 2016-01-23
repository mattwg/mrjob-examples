from mrjob.job import MRJob
import mrjob
import pdb

# With combiner

class MRCount(MRJob):

    def mapper(self, _, line):
        # Each line is CSV
        # Skip header and emit month and count
        l = [s.strip('"') for s in line.split(',')]
        if l[0] != '' and l[7] != 'NA':
            # strip off quotes
            yield l[12], int(l[7])

    def combiner(self, origin, delay):
        # Less network traffic to reducer - sufficient stats for mean
        d = list(delay)
        d_sum = float(sum(d))
        d_n = len(d)
        d_mean = d_sum / d_n
        if d_n > 1:
            d_var = sum( (x - d_mean) ** 2 for x in d ) / ( d_n - 1 )
        else:
            d_var = 0
        yield origin, ( d_mean, d_var, d_n)

    def reducer(self, origin, tuples):
        # Convert generator to list - multiple passes required
        # Compute weighted mean
        t = list(tuples)
        t_sum = sum( x[0] * x[2] for x in t )
        t_n = sum(x[2] for x in t)
        t_mean = t_sum / t_n
        n_rows = len(t)
        if n_rows > 1:
            mu = [ x[0] for x in t]
            var = [ x[1] for x in t]
            n = [ x[2] for x in t]

            sum_n_s2 = sum(n * (x ** 2))
            # [(i,j) for i in range(0,3) for j in range(0,3) if i < j]
            # See https://en.wikipedia.org/wiki/Standard_deviation
            sum_i_less_j = [ n[i] * n[j] * ( mu[i] - mu[j]) ** 2
                for i,x in range(0,n_rows)
                for j in range(0,n_rows3) if i < j]

            t_sd = ( (sum_n_s2/t_n) + (sum_i_less_j/(t_n**2)) ) ** 0.5
        elif n_rows == 1:
            t_sd = t[0][1]**0.5
        else:
            t_sd = None

        yield origin, ( t_mean, t_sd)

if __name__ == '__main__':
    MRCount.run()
