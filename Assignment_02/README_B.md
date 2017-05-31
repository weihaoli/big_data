### Purpose

* wirte a program that read in date as formate "yyyy-mm-dd"
* output the estimated daily transaction value, including the charge that returns to the sender itself
### approach
* choosing between "get" from web api and the python api provided by the bitcoin.info for simplicity 
* Use library request later for satbality
* looping trhough each blocks in a day and check the transactions in it, and sum them up
### problem encountered
* the python api is not well functioning for some specific transactions, which will return key error"addr" 
* debug: just use python api to get the hash, after that, use the web api to get de raw block and read data from it.
