from mrjob.job import MRJob
import mrjob
import pdb

class MRFilter(MRJob):

    # We want raw value only output - no key
    OUTPUT_PROTOCOL = mrjob.protocol.RawValueProtocol

    def mapper(self, _, line):
        # Each line is CSV
        # Skip header and output just 1st Feb
        l = [s.strip('"') for s in line.split(',')]
        if ( l[0] != '' and l[2] == '2' and l[3] == '1'):
            # strip off quotes
            yield line.replace('"',''), None

    def reducer(self, lines, _):
        yield None, lines

if __name__ == '__main__':
    MRFilter.run()
