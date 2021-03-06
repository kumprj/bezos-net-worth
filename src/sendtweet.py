import os
from twython import Twython
import requests
import random
import psycopg2
import datetime

from dateutil.relativedelta import relativedelta
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
#     polygon_api_key,
#     share_count
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
polygon_api_key = os.environ['polygon_api_key']

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
    
# Aggregate bars strategy, if desired:
# today_url = f'https://api.polygon.io/v2/aggs/ticker/amzn/range/1/day/{today}/{today}?apiKey={polygon_api_key}'
def get_prices():
    today = datetime.date.today()
    if today.weekday() != 0:
        yesterday = today - datetime.timedelta(days=1)
    else:
        # If Today is Monday we want Friday
        yesterday = today - datetime.timedelta(days=3)

    # Today Value    
    today_url = f'https://api.polygon.io/v1/last/stocks/AMZN?apiKey={polygon_api_key}'
    today_response = requests.get(today_url).json()

    # Yesterday Value
    yesterday_url = f'https://api.polygon.io/v1/open-close/AMZN/{yesterday}?apiKey={polygon_api_key}'
    amzn_yesterday_json = requests.get(yesterday_url).json()
    
    # Verify our data returned. For example, if we hit a holiday as the previous day, we'd receive invalid json.
    json_valid = verify_json(amzn_yesterday_json)
    if json_valid == True:
        amzn_yesterday_json = amzn_yesterday_json
    elif json_valid == False:
        raise ValueError('Invalid JSON returned')
    elif json_valid['status'] == 'OK':
        amzn_yesterday_json = json_valid

    # After validating our data, grab the prices to return.
    amzn_close_today = today_response['last']['price']
    amzn_yesterday_close = amzn_yesterday_json['close']

    # Print our dates and closes for logging purposes
    print(today)
    print(yesterday)
    print(today_response)
    print(amzn_yesterday_json)
    return amzn_close_today, amzn_yesterday_close

# Today we are checking for last trade price. 
# For yesterday, we're checking the previous trading day. There are holidays that cause issues with this,
# so this function handles that edge case.
def verify_json(yesterday):
    if yesterday['status'] == 'OK':
        return True

    # Start at 4, because the above True would hit if '3' was valid json.
    i = 4
    while i < 10: # arbitrary 10, we just need a terminator
        current = datetime.date.today() - datetime.timedelta(days=i) 
        url = f'https://api.polygon.io/v1/open-close/AMZN/{current}?apiKey={polygon_api_key}'
        resp = requests.get(url).json()

        if resp['status'] == 'OK':
            i = 10
            return resp
        # else increment
        i += 1
    # If all of the above fails and for some reason 
    # we can't find data in the last 10 days, return False.
    return False


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
        today = datetime.datetime.now().date()
        three_months = datetime.timedelta(3*365/12)
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
    twitter.update_status(status=tweet_text)
    update_db_date(num_id, str_id, last_use)

    # Print statements for logging purposes.
    print(tweet_text)
    print(closing_price)
    print(prev_day_close)
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
    today = datetime.datetime.now().date()
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
