import re
import requests
import pandas as pd
import twint
from datetime import datetime, timedelta
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from time import sleep
from bs4 import BeautifulSoup

import google_sheets


def get_usernames_list():
    api_url = "https://api.theprawns.xyz/api/v1/projects"
    response = requests.get(api_url)
    response = response.json()
    usernames_list = []
    for projects in response['data']:
        if not projects['collectionObject']['twitter_username']:
            pass
        else:
            usernames_list.append(projects['collectionObject']['twitter_username'])
    return set(usernames_list)


def urls_to_scrape(username):
    dataframe_array = []
    c = twint.Config()
    c.Username = f'{username}'
    c.Search = ''
    c.Retweets = False
    c.Since = since
    c.Until = until
    c.Hide_output = True
    c.Pandas = True
    twint.run.Search(c)
    dataframe = twint.storage.panda.Tweets_df
    try:
        dataframe = dataframe[['username', 'link', 'urls']]
        dataframe = dataframe.astype({'urls': 'string'})

        for index in dataframe.index:
            if "https://twitter.com/i/space" not in dataframe['urls'][index]:
                dataframe = dataframe.drop(index=index)

        dataframe['urls'] = dataframe['urls'].str.replace("[", "", regex=True)
        dataframe['urls'] = dataframe['urls'].str.replace("]", "", regex=True)
        dataframe['urls'] = dataframe['urls'].str.replace("'", "", regex=True)
        dataframe = dataframe.astype({'urls': 'object'})
        dataframe_array.append(dataframe)

    except Exception as e:
        print(str(e))

    df_to_scrape = pd.pandas.DataFrame(dataframe).reset_index(drop=True)
    df_to_scrape = df_to_scrape.drop_duplicates(subset=['urls'], keep='first')
    print(df_to_scrape)

    return df_to_scrape


def launch_website(url):
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.get(url)
    driver.maximize_window()
    return driver


def get_space_data(driver):
    soup = BeautifulSoup(driver.page_source, 'lxml')

    if soup.find('div', class_=re.compile('r-1vr29t4')) is not None:
        spaces_text = soup.find('div', class_=re.compile('r-1vr29t4')).text
    else:
        spaces_text = ''

    if soup.find('div', class_='css-1dbjc4n r-1d09ksm r-xoduu5 r-18u37iz r-1wbh5a2') is not None:
        twitter_space_date = soup.find('div', class_='css-1dbjc4n r-1d09ksm r-xoduu5 r-18u37iz r-1wbh5a2').text
        twitter_space_date = twitter_space_date.replace(" at", "")
        if "today" in twitter_space_date:
            twitter_space_date = twitter_space_date.replace("today", today)
        if "tomorrow" in twitter_space_date:
            twitter_space_date = twitter_space_date.replace("tomorrow", tomorrow)
        if "Ended" in twitter_space_date:
            twitter_space_date = ''
    else:
        twitter_space_date = ''

    if soup.find('span', class_='css-901oao css-16my406 css-1hf3ou5 r-poiln3 r-bcqeeo r-qvutc0') is not None:
        host_name = soup.find('span', class_='css-901oao css-16my406 css-1hf3ou5 r-poiln3 r-bcqeeo r-qvutc0').text
    else:
        host_name = ''

    return spaces_text, twitter_space_date, host_name


def get_tweet_data(driver):
    soup = BeautifulSoup(driver.page_source, 'lxml')

    if soup.find('div', class_='css-901oao r-1nao33i r-37j5jr r-1blvdjr r-16dba41 r-vrz42v '
                               'r-bcqeeo r-bnwqim r-qvutc0') is not None:

        tweet_text = soup.find('div', class_='css-901oao r-1nao33i r-37j5jr r-1blvdjr r-16dba41 r-vrz42v '
                                             'r-bcqeeo r-bnwqim r-qvutc0').text
        tweet_text = tweet_text.replace('\n\n', '\n')
        tweet_text = tweet_text.replace('\n', '')
        tweet_text = tweet_text.replace('  ', ' ')
    else:
        tweet_text = ''

    return tweet_text


def scrape(df_to_scrape):
    url_array = []
    if len(df_to_scrape) == 0:
        print("There have not been any twitter spaces scheduled in the last 24 hours!")

    for i in range(len(df_to_scrape)):
        tweet_url = df_to_scrape.iloc[[i]]['link'].values
        spaces_url = df_to_scrape.iloc[[i]]['urls'].values
        subarray = [tweet_url[0], spaces_url[0]]
        url_array.append(subarray)

    print(url_array)
    return url_array


if __name__ == "__main__":
    since = (datetime.now() - timedelta(5)).strftime('%Y-%m-%d %H:%M:%S')
    until = (datetime.now()).strftime('%Y-%m-%d %H:%M:%S')

    today = (datetime.now()).strftime('%Y-%m-%d')
    tomorrow = (datetime.now() + timedelta(1)).strftime('%Y-%m-%d')

    data_to_upload = pd.DataFrame(columns=['username', 'link', 'urls', 'host_name', 'tweet_text', 'spaces_text',
                                           'twitter_space_dates'])
    usernames_list = get_usernames_list()
    for username in usernames_list:
        df_to_scrape = urls_to_scrape(username)
        url_array = scrape(df_to_scrape)

        tweet_text_array = []
        spaces_text_array = []
        twitter_space_dates_array = []
        host_name_array = []

        if len(url_array) == 0:
            tweet_text_array = None
            spaces_text_array = None
            twitter_space_dates_array = None
            host_name_array = None
        else:
            for url in url_array:
                driver = launch_website(url[1])
                sleep(10)
                spaces_text, twitter_space_date, host_name = get_space_data(driver)
                sleep(1)
                print(spaces_text)
                print(twitter_space_date)
                print(host_name)
                driver.quit()

                driver = launch_website(url[0])
                sleep(10)
                tweet_text = get_tweet_data(driver)
                sleep(1)
                print(tweet_text)
                driver.quit()

                if "tuned in" not in twitter_space_date and host_name is not [''] and twitter_space_date is not [''] \
                        and "in this space" not in twitter_space_date:
                    tweet_text_array.append(tweet_text)
                    spaces_text_array.append(spaces_text)
                    twitter_space_dates_array.append(twitter_space_date)
                    host_name_array.append(host_name)
                else:
                    df_to_scrape = df_to_scrape[df_to_scrape.link != url[0]]
                    df_to_scrape = df_to_scrape.reset_index(drop=True)

        if tweet_text_array is None or spaces_text_array is None or twitter_space_dates_array is None or host_name_array is None:
            pass
        elif not tweet_text_array and not spaces_text_array and not twitter_space_dates_array and not host_name_array:
            pass
        else:
            print("---------------")
            print(tweet_text_array)
            print(spaces_text_array)
            print(twitter_space_dates_array)
            print(host_name_array)
            print("---------------")
            df_tweet_text = pd.DataFrame(tweet_text_array, columns=['tweet_text'])
            df_spaces_text = pd.DataFrame(spaces_text_array, columns=['spaces_text'])
            df_twitter_space_dates = pd.DataFrame(twitter_space_dates_array, columns=['twitter_space_dates'])
            df_host_names = pd.DataFrame(host_name_array, columns=['host_name'])
            print("---------------")
            print(df_to_scrape)
            final_df = df_to_scrape.join(df_host_names)
            final_df = final_df.join(df_tweet_text)
            final_df = final_df.join(df_spaces_text)
            final_df = final_df.join(df_twitter_space_dates)
            print(final_df)
            print("---------------")
            data_to_upload = pd.concat([data_to_upload, final_df])

    print("---------------")
    print(data_to_upload)
    if not data_to_upload.empty:
        print("--- Uploading to Google Sheet ---")
        creds = google_sheets.authorize()
        google_sheets.update_gs(data_to_upload, creds)
    else:
        print("--- Nothing to upload to Google Sheet ---")
        pass
