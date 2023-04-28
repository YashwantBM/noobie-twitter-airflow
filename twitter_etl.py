import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs

def run_twitter_etl():


    access_key = "AKIASJPFGW33XSE7BM6S"
    access_secret = "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"
    consumer_key = "3188025978-GQhDf98cLWjTbrAejtJI2qztKkzyrfxeNlOmxlZ"
    consumer_secret = "3LWWfM7pIGO0Kcf5d4SCoQ5381xjwbpjwTALmFX4HMa6K"

    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key, consumer_secret)

    api = tweepy.API(auth)

    tweets =  api.user_timeline(screen_name="@iamsrk",
                                    count=200,
                                    include_arts=False,
                                    tweet_mode= 'extended'
                                    )

    tweet_list =[]
    for tweet in tweets:
            text = tweet._json["full_text"]

            redefined_tweet = {"user": tweet.user.screen_name,
                            'text' : text,
                            'favorite_count': tweet.favorite_count,
                            'retweet_count': tweet.retweet_count,
                            'created_at': tweet.created_at}

            tweet_list.append(redefined_tweet)


    df = pd.DataFrame(tweet_list)
    df.to_csv("s3://airflow-twitterapi-project/iamsrk_twitter_data.csv")