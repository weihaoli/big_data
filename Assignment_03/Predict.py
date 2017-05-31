from pyspark.sql import SparkSession
import pandas as pd
import re

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()\

#load dictionary
def load_dict():
    df = pd.read_excel("Dic_2014.xlsx",  parse_cols='A,E')
    df['Word'] = df['Word'].astype(str)
    df['Score'] = df['Score'].astype(int)
    _dic = df.set_index('Word').T.to_dict('list')
    return _dic

#load Json.bz2
def load_data(i):
    path =i
    twtr = spark.read.json(path)
    twtr = twtr.filter(twtr['lang'] == 'en')
    twtr_rdd = twtr.rdd
    return twtr_rdd


# evaluate the word depends on dictionary
def word_eva(clean_text, scores):
    scores['M'] = scores['M'] + 1
    list_words = clean_text.split(" ")
    for word in list_words:
        if word.upper() in senti_dic and senti_dic[word.upper()][0] > 0:
            scores['P'] = scores['P'] + 1
            return 0
        if word.upper() in senti_dic and senti_dic[word.upper()][0] < 0:
            scores['N'] = scores['N'] + 1
            return 0
    return 0


#Map function in mapreduce
# map function
def map_dict(x):

    # dictionary that records scores
    score_dic = {'AMZN': {'M': 0, 'P': 0, 'N': 0}, 'IBM': {'M': 0, 'P': 0, 'N': 0}, \
                 'TSLA': {'M': 0, 'P': 0, 'N': 0}, "MSFT": {'M': 0, 'P': 0, 'N': 0}, \
                 'VZ': {'M': 0, 'P': 0, 'N': 0}}

    # the part process ,
    in_str = str(x)

    clean_text = str(' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|(text=u)", \
                                     " ", x['text']).split()))

    # if the tick name is in the clean test that I evaluate the test
    for i in ["AMZN", "IBM", "TSLA", "VZ", "MSFT"]:
        if i in clean_text:
            word_eva(clean_text, score_dic[i])

    return score_dic
#Reduce function
#summary the scores
def reduce_dict(x,y):
    new_dict = x
    for i in x:
        for j in x[i]:
            new_dict[i][j] = x[i][j]+y[i][j]
    return new_dict

#main part: loop through each date and recored the scores

senti_dic = load_dict()
#change here for new path
twtr_rdd = load_data("12/01/*/*.json.bz2")
final_score = twtr_rdd.map(map_dict).reduce(reduce_dict)

stock_price = []
stock_price.append( -0.009083 + 0.000145*final_score['IBM']['M'] -0.000575*final_score['IBM']['P'])
stock_price.append( -0.022656 + 0.001149*final_score['AMZN']['M']-0.003193*final_score['AMZN']['P'])
stock_price.append(  0.008772 - 0.000383*final_score['VZ']['M']  -0.000846*final_score['VZ']['P'])
stock_price.append(  0.006538 - 0.000337*final_score['MSFT']['M']-0.000574*final_score['MSFT']['P'])
stock_price.append(  0.020185 - 0.002019*final_score['TSLA']['M']+0.000463*final_score['TSLA']['P'])

print "predcited price for IBM,AMZN,VZ,MSFT,TSLA",stock_price
