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
#     td_key,
#     database_user,
#     database_password,
#     database_host,
#     database_port,
#     database_db,
#     share_count,
#     client_id,
#     refresh_token
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
td_key = os.environ['td_key']
refresh_token = os.environ['refresh_token']
client_id = os.environ['client_id']

twitter = Twython(
    consumer_key,
    consumer_secret,
    access_token,
    access_token_secret
)
current_price_api_url = f"https://api.tdameritrade.com/v1/marketdata/AMZN/quotes?apikey={td_key}"
yesterday_price_api_url = f"https://api.tdameritrade.com/v1/marketdata/AMZN/pricehistory?apikey={td_key}&periodType=month&period=1&frequencyType=daily&frequency=1&needExtendedHoursData=false"

def rds_connect():
    return psycopg2.connect(user = database_user,
                            password = database_password,
                            host = database_host,
                            port = database_port,
                            database = database_db)

def get_token():
    token_url = "https://api.tdameritrade.com/v1/oauth2/token"
    headers = {"Content-Type" : "application/x-www-form-urlencoded"}
    data = f"grant_type=refresh_token&refresh_token={refresh_token}&access_type=&code=&client_id={client_id}&redirect_uri="
    token = requests.post(token_url, headers=headers, data=data)
    output = token.json()
    return output["access_token"]
    
def main():
    token = get_token()
    headers = {}
    headers["Authorization"] = f"Bearer {token}"
    resp = requests.get(current_price_api_url, headers=headers)
    amzn_json = resp.json()

    closing_price = amzn_json['AMZN']['lastPrice']
    net_worth = int(closing_price * int(share_count))
    net_worth_str = "{:,}".format(net_worth)
    net_worth_str = net_worth_str[0:3]

    prev_day_resp = requests.get(yesterday_price_api_url, headers=headers)
    prev_day_json = prev_day_resp.json()

    prev_day_close = prev_day_json['candles'][-1]['close']
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

if __name__ == "__main__":
    main()
