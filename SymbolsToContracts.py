import csv
import time
import logging

import Contracts

logger = logging.getLogger('SymbolsToContracts')
logger.setLevel(logging.DEBUG)

def load(symbolsFile):
    with open(symbolsFile, 'r') as fin:
        reader = csv.reader(fin)
        for row in reader:
            logger.debug("Requesting contract for %s", row[0])
            Contracts.request(row[0])
            time.sleep(.2)
