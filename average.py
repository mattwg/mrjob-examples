from mrjob.job import MRJob
import mrjob
import pdb

class MRCount(MRJob):

    def mapper(self, _, line):
        # Each line is CSV
        # Skip header and emit month and count
        l = [s.strip('"') for s in line.split(',')]
        if l[0] != '':
            # strip off quotes
            yield int(l[2]), 1

    def combiner(self, month, count):
        yield month, sum(count)

    def reducer(self, month, count):
        yield month, sum(count)

if __name__ == '__main__':
    MRCount.run()
