from twython import Twython
import requests
import random
import psycopg2
import datetime
from dateutil.relativedelta import relativedelta
from settings import (
    consumer_key,
    consumer_secret,
    access_token,
    access_token_secret,
    td_key,
    database_user,
    database_password,
    database_host,
    database_port,
    database_db,
    share_count
)

twitter = Twython(
    consumer_key,
    consumer_secret,
    access_token,
    access_token_secret
)
apiUrl = f"https://api.tdameritrade.com/v1/marketdata/AMZN/quotes?apikey={td_key}"
apiUrl2 = f"https://api.tdameritrade.com/v1/marketdata/AMZN/pricehistory?apikey={td_key}&periodType=month&period=1&frequencyType=daily&frequency=1&needExtendedHoursData=false"

def rds_connect():
    return psycopg2.connect(user = database_user,
                            password = database_password,
                            host = database_host,
                            port = database_port,
                            database = database_db)

def main():
    resp = requests.get(apiUrl)
    amzn_json = resp.json()
    closing_price = amzn_json['AMZN']['lastPrice']
    net_worth = int(closing_price * int(share_count))
    net_worth_str = "{:,}".format(net_worth)

    prev_day_resp = requests.get(apiUrl2)
    prev_day_json = prev_day_resp.json()
    prev_day_close = prev_day_json['candles'][-1]['close']
    prev_worth = int(prev_day_close * int(share_count))
    prev_worth_str = "{:,}".format(prev_worth)

    net_change = abs(prev_worth - net_worth)
    net_change_str = "{:,}".format(net_change)

    up_down = 'down' if (prev_day_close > closing_price) else 'up'
    gain_loss = 'loss' if (prev_day_close > closing_price) else 'gain'
    recently_used = True

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

    amount = int(net_change / item_cost)
    amount_str =  "{:,}".format(amount)
    tweet_text = f"Today Jeff's $AMZN shares are worth ${net_worth_str} billion, {up_down} from ${prev_worth_str} billion yesterday (share price {prev_day_close} -> {closing_price}). This is a change of ${net_change_str} and a {gain_loss} of {amount_str} {tweet_text_from_db}."
    twitter.update_status(status=tweet_text)
    update_db_date(num_id, str_id, last_use)


def select_tweet():
    connection = rds_connect()
    cursor = connection.cursor()
    # id = random.randint(1,5)
    id = 0
    select_query = f'select tweettext, item_cost, last_use, num_id, str_id from public.bezostweets where num_id = {id}'
    cursor.execute(select_query)
    db_results = cursor.fetchone() # Fetch the end time value from our table if it exists. Point of restart for the script. Only ever one element here.
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
