import twint
import pandas as pd
import json

df = pd.read_csv("exploTwint.csv")
handles = df["handles"].tolist()

all_tweets = []

for handle in handles:
    print(handle)
    c = twint.Config()
    c.Username = handle
    c.Since = "2023-04-10"
    c.Limit = 100
    c.Store_csv = True
    c.Output = "none.csv"
    c.Lang = "en"
    c.Translate = True
    c.TranslateDest = "it"
    twint.run.Search(c)

    twint.run.Search(c)

    #tweets = twint.output.tweets_list
    #all_tweets += tweets

with open("tweets.json", "w") as outfile:
    json.dump(all_tweets, outfile)