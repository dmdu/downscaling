import time
import sys

class DiscardedCyclesCounter(object):

    def __init__(self, file_name):

        self.file_name = file_name

    def count_seconds(self):
        sum = 0
        with open(self.file_name) as file_object:
            for line in file_object:
                items = line.split(',')
                #print items[3]
                sum += int(items[3])
        return sum

if __name__ == '__main__':
    counter = DiscardedCyclesCounter(sys.argv[1])
    print counter.count_seconds()