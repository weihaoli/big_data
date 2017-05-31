#daily-bit

from blockchain import blockexplorer
import argparse
from datetime import datetime, date
import requests
import json

#parse date

parser = argparse.ArgumentParser()
parser.add_argument("date")
args = parser.parse_args()
y,m,d = args.date.split('-')
y = int(y)
m = int(m)
d = int(d)
print y,m,d
x =  int(( (datetime(y,m,d) - datetime(1970, 1, 1)).total_seconds())*1000)

#get blocks->block->transaction->output->value
def BTC_per_day(date):
    count = 0
    blocks = blockexplorer.get_blocks(date)
    for s_block in blocks:
        hash_key = s_block.hash

        block = json.loads(requests.get('https://blockchain.info/rawblock/'+hash_key).content)
        for tran in  block['tx']:
            for output in tran['out']:
                count += output['value']
    return float(count/1e8)


print 'There are',BTC_per_day(x),"traded at that Date."




