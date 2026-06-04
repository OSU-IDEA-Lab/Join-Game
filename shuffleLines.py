import random
import argparse
import sys
import os
import time

parser = argparse.ArgumentParser()
parser.add_argument('file_name', help='Provide the file name')
args = parser.parse_args()

inputFilename = args.file_name

if not os.path.exists(inputFilename):
    print(f"The file {inputFilename} does not exist. Quitting...".format(inputFilename))
    sys.exit()


class ShuffleLine:

    def __init__(self, inp_file):
        self.file_name = inp_file

    # Shuffling with help of list of tuples
    def shuffle_read(self):
        file_name = self.file_name
        with open(file_name, 'r') as f:
            data = [(random.random(), line) for line in f]
        data.sort()
        with open(file_name, 'w') as target:
            for _, line in data:
                target.write(line)
        return len(data)

    # Shuffling via random.shuffle(method) and then writing the shuffled
    # content to the same file
    def shuffle_readline(self):
        file_name = self.file_name
        with open(file_name, 'r') as f:
            lines = f.readlines()
            random.shuffle(lines)

        with open(file_name, 'w') as f:
            f.writelines(lines)

        return len(lines)


def main():

    obj = ShuffleLine(inputFilename)

    print('Method 1')
    t1 = time.time()
    num_lines = obj.shuffle_readline()
    t2 = time.time()
    print(f'Total time taken {t2-t1} seconds to shuffle {num_lines} lines.')

    #print('\nMethod 2')
    #t1 = time.time()
    #num_lines = obj.shuffle_read()
    #t2 = time.time()
    #print(f'Total time taken {t2-t1} seconds to shuffle {num_lines} lines.')


if __name__ == '__main__':
    main()

