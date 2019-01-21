from mrjob.job import MRJob

class MR (MRJob) :
    def mapper(self, key, line):
        _, _, rating , _ = line.split('\t')
        yield rating , 1
    def reducer(self, key, values):
        yield key , sum(values)


if __name__ == '__main__':
    MR.run()