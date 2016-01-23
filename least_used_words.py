from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
import re
import pdb


WORD_RE = re.compile(r"[\w']+")

class MRLeastUsedWord(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_min_words)
        ]

    def mapper_get_words(self, _, line):
        # yield each word in the line
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)

    def combiner_count_words(self, word, counts):
        # optimization: sum the words we've seen so far
        yield (word, sum(counts))

    def reducer_count_words(self, word, counts):
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.
        yield None, (sum(counts), word)

    # discard the key; it is just None
    def reducer_find_min_words(self, _, word_count_pairs):
        # each item of word_count_pairs is (count, word),
        # filter where word_count is min - may be more than one
        # need to side step the generator function!
        pairs = list(word_count_pairs)
        min_count = int(min(pairs)[0])

        for p in filter(lambda x: x[0] == min_count, pairs):
            yield p


if __name__ == '__main__':
    MRLeastUsedWord.run()
