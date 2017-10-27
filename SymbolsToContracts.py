import csv
import time

import Contracts

def load(symbolsFile):
    with open(symbolsFile, 'r') as fin:
        reader = csv.reader(fin)
        for row in reader:
            print ("Requesting contract for", row[0])
            Contracts.request(row[0])
            time.sleep(.2)
