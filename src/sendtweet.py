import os
from twython import Twython
import random
import psycopg2
import datetime
import pandas as pd
from datetime import datetime, timedelta

# For Local Development
# from settings import (
#     consumer_key,
#     consumer_secret,
#     access_token,
#     access_token_secret,
#     database_user,
#     database_password,
#     database_host,
#     database_port,
#     database_db,
#     share_count,
#     apiKey
# )

access_token = os.environ['access_token']
access_token_secret = os.environ['access_token_secret']
consumer_key = os.environ['consumer_key']
consumer_secret = os.environ['consumer_secret']
database_db = os.environ['database_db']
database_host = os.environ['database_host']
database_password = os.environ['database_password']
database_port = os.environ['database_port']
database_user = os.environ['database_user']
share_count = os.environ['share_count']
apiKey = os.environ['apiKey']

twitter = Twython(
    consumer_key,
    consumer_secret,
    access_token,
    access_token_secret
)

def rds_connect():
    return psycopg2.connect(user = database_user,
                            password = database_password,
                            host = database_host,
                            port = database_port,
                            database = database_db)
    
def get_prices():

    eDate = int((datetime.today() - datetime(1970,1,1)).total_seconds())
    sDate = int(((datetime.today() - timedelta(days=20)) - datetime(1970,1,1)).total_seconds())
    symbol = 'AMZN'
    url = 'https://finnhub.io/api/v1/stock/candle?symbol=' + symbol + '&resolution=D&from='+ str(sDate) +'&to=' + str(eDate) + '&token=' + apiKey
    df = pd.read_json(url)

    # print(df)
    # print(df.iloc[-2]['c']) # Yesterday
    # print(df.iloc[-1]['c']) # Today
    yesterday = df.iloc[-2]['c']
    today = df.iloc[-1]['c']
    
    return today, yesterday


def main():
    closing_price, prev_day_close = get_prices()

    # Today Net Worth
    net_worth = int(closing_price * int(share_count))
    net_worth_str = "{:,}".format(net_worth)
    net_worth_str = net_worth_str[0:3]

    # Yesterday Net Worth
    prev_worth = int(prev_day_close * int(share_count))
    prev_worth_str = "{:,}".format(prev_worth)
    prev_worth_str = prev_worth_str[0:3]

    # Format net worth with commas. 
    net_change = abs(prev_worth - net_worth)
    net_change_str = "{:,}".format(net_change)

    # Calculate if they are unrealized gains or losses.
    up_down = 'down' if (prev_day_close > closing_price) else 'up'
    gain_loss = 'loss' if (prev_day_close > closing_price) else 'gain'
    recently_used = True

    # Loop through our database options to identify some text for today's tweet. Ensure it hasn't 
    # been used in the last 3 months.
    while recently_used:
        tweet_text_from_db, item_cost, last_use, num_id, str_id = select_tweet()
        today = datetime.now().date()
        three_months = timedelta(3*365/12)
        three_months_ago = today - three_months

        if three_months_ago < last_use:
            continue
        else:
            recently_used = False
            break

    # Calculate the amount of everyday items, adding commas to nicely format it.
    amount = int(net_change / item_cost)
    amount_str = "{:,}".format(amount) if amount >= 1000 else str(amount)

    # Send the tweet.
    tweet_text = f"Today Jeff's $AMZN shares are worth ${net_worth_str} billion, {up_down} from ${prev_worth_str} billion yesterday. This is a {gain_loss} of ${net_change_str} and the equivalent of {amount_str} {tweet_text_from_db}."
    # twitter.update_status(status=tweet_text)
    # update_db_date(num_id, str_id, last_use)

    # Print statements for logging purposes.
    print(tweet_text)
    print(f'Todays closing price is {closing_price}')
    print(f'Yesterdays closing price is {prev_day_close}')
    print(share_count)


def get_content_count():
    connection = rds_connect()
    cursor = connection.cursor()
    select_query = f'select COUNT(num_id) from public.bezostweets'
    cursor.execute(select_query)
    db_results = cursor.fetchone()
    cursor.close()
    connection.close()
    return db_results[0]

def select_tweet():
    connection = rds_connect()
    cursor = connection.cursor()
    count = get_content_count() - 1 # Set upper bound based on db column size.
    id = random.randint(0, count) # Inclusive
    select_query = f'select tweettext, item_cost, last_use, num_id, str_id from public.bezostweets where num_id = {id}'
    cursor.execute(select_query)
    db_results = cursor.fetchone()
    cursor.close()
    connection.close()
    return db_results[0], db_results[1], db_results[2], db_results[3], db_results[4]


def update_db_date(num_id, str_id, last_use):
    connection = rds_connect()
    cursor = connection.cursor()
    today = datetime.now().date()
    insert_query = f'''insert into public.bezostweets (num_id, str_id, last_use) values ({num_id}, '{str_id}', '{last_use}')
                        ON CONFLICT (num_id, str_id) DO UPDATE SET last_use = '{today}';'''
    cursor.execute(insert_query)
    connection.commit()
    cursor.close()
    connection.close()

# Handler
def my_handler(event, context):
    main()

# For Local dev
# if __name__ == "__main__":
#     main()
