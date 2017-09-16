import requests
import mysql.connector as mysql
import datetime
from configparser import ConfigParser
from dateutil import parser
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session


def generate_media_queue():
    sql = "CALL nm_spSocialMediaBufferQueue()"
    cursor.execute(sql)
    conn.commit()


def buffer_post(profile, content, url):
    parser = ConfigParser()
    parser.read("whconfig.ini")
    print('begin!!')
    client_id = parser.get('BUFFER', 'client_id')
    buffer_access = parser.get('BUFFER', 'access')
    buffer_profile = parser.get('BUFFER', profile)

    client = BackendApplicationClient(client_id=client_id)
    oauth = OAuth2Session(token=buffer_access, client=client)
    auth_code = oauth.token
    nm_archive = 0

    if profile == 'Twitter':
        if nm_archive == 1:
            requests.post("https://api.bufferapp.com/1/updates/create.json?access_token=" + auth_code,
                          data={'profile_ids': buffer_profile,
                                'text': 'From the archive: ' + content + ' ' + url})
        if nm_archive == 0:
            requests.post("https://api.bufferapp.com/1/updates/create.json?access_token=" + auth_code,
                          data={'profile_ids': buffer_profile,
                                'text': content + ' ' + url})

    if profile == 'Facebook':
        requests.post("https://api.bufferapp.com/1/updates/create.json?access_token=" + auth_code,
                      data={'profile_ids': buffer_profile,
                            'text': content + ' ' + url,
                            'link': url})


def daily_loads():
    today = datetime.datetime.today().strftime('%Y-%m-%d')
    cursor.execute("SELECT COUNT(*) FROM NovaraWH.WebsiteSocialMediaHistory a WHERE PostingDate = %s", [today])
    count = cursor.fetchall()[0][0]

    cursor.execute("""SELECT    a.PageTitle,	-- 0
                                a.PageTypePrimary,	-- 1
                                a.PageTypeSecondary,	-- 2
                                a.PostingDate,	-- 3
                                a.NovaraArchive,	-- 4
                                a.Platform,	-- 5
                                a.PostType,	-- 6
                                b.URL,	-- 7
                                b.PageDesc,	-- 8
                                b.TweetText,	-- 9
                                b.FacebookText	-- 10
                                FROM NovaraWH.WebsiteSocialMediaHistory a
                                LEFT JOIN NovaraWH.WebsitePages b ON a.PageID = b.ID
                                WHERE PostingDate = %s
                                ORDER BY RAND()""", [today])
    social_queue = cursor.fetchall()
    n = 0

    for i in social_queue:

        content = ''

        platform = social_queue[n][5]
        post_type = social_queue[n][6]
        page_url = social_queue[n][7]

        if social_queue[n][9] != '':
            tweet_text = social_queue[n][9]
        else:
            tweet_text = social_queue[n][0]

        if social_queue[n][10] != '':
            fb_text = social_queue[n][10]
        else:
            fb_text = social_queue[n][8]

        if platform == 'Twitter':
            content = tweet_text
        elif platform == 'Facebook':
            content = fb_text

        if post_type == 'Archive' and len(tweet_text) < 103:
            content = 'From the archive: ' + content
        elif post_type == 'Archive' and len(tweet_text) > 102:
            content = 'From the archive: ' + social_queue[n][0]

        buffer_post(platform, content, page_url)

        n = + 1


def main_proc():
    generate_media_queue()
    daily_loads()

global cursor

parser = ConfigParser()
parser.read("whconfig.ini")

wh_host = parser.get('MYSQL', 'host')
wh_user = parser.get('MYSQL', 'user')
wh_pass = parser.get('MYSQL', 'password')

conn = mysql.connect(host=wh_host,
                     user=wh_user,
                     password=wh_pass,
                     database='NovaraWH')

cursor = conn.cursor()

main_proc()