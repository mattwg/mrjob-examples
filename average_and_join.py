from mrjob.job import MRJob
import mrjob
import pdb
import re

# No combiner!

class MRCount(MRJob):

    def mapper(self, _, line):
        # Each line is CSV
        # If table has two columns then it is the RH join table
        # If table has > 2 columns skip header and emit month and count
        clean_line = re.sub(r',(?=[^"]*"[^"]*(?:"[^"]*"[^"]*)*$)','',line)

        l = [s.strip('"') for s in clean_line.split(',')]
        if len(l) == 2:
            yield l[1], ('R',l[0])
        elif len(l) == 17:
            if l[0] != '' and l[7] != 'NA':
                # strip off quotes
                yield l[12], ('L',int(l[7]))


    def reducer(self, origin, tuples):
        # We have 'left' and 'right' tables
        # Convert generator to list - multiple passes required
        t = list(tuples)
        tl = [x[1] for x in t if x[0] == 'L']
        tr = [x[1] for x in t if x[0] == 'R']

        if len(tl) > 0 and len(tr) > 0:
            tl_sum = float(sum(tl))
            tl_n = len(tl)
            tl_mean = tl_sum / tl_n
            if tl_n > 1:
                tl_sd = ( sum( (x - tl_mean) ** 2 for x in tl ) / ( tl_n - 1 ) ) ** 0.5
            else:
                tl_sd = None
            yield origin, ( tr[0], tl_mean, tl_sd )

if __name__ == '__main__':
    MRCount.run()
