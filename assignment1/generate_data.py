import argparse
import csv
import gzip
import io
import random
import sys
import uuid


if sys.version_info.major < 3:
    wrapper = lambda x: x
else:
    wrapper = io.TextIOWrapper


def generate_data(scale):
    print('Generating dataset with scale: %s' % scale)

    products = list(range(scale * 2**8))
    try:
        with gzip.open('data_%s.csv.gz' % scale, 'w') as f:
            writer = csv.writer(wrapper(f), delimiter=',')
            for i in range(scale * 2**16):
                basket = uuid.uuid4()
                for product in random.sample(products, random.randint(1, 5)):
                    writer.writerow([basket, product])
    except Exception as err:
        print('Generating the dataset did not work: %s' % err)
        return False

    return True


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate randomized basket data.')
    parser.add_argument('--scale', type=int, default=1, choices=range(1, 11),
                        help='Scale parameter for the data to be generated (within range 1-10).')
    args = parser.parse_args()

    success = generate_data(args.scale)
    if success:
        print('Generated data file.')
    else:
        print('Could not generate data file.')
