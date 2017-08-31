import requests
import codecs
import html
import json
import time
import mysql.connector as mysql
from configparser import ConfigParser
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from urllib.request import Request, urlopen
from dateutil import parser


def buffer_post(profile, content, url):
    parser = ConfigParser()
    parser.read("whconfig.ini")

    client_id = parser.get('BUFFER', 'client_id')
    buffer_access = parser.get('BUFFER', 'access')
    buffer_profile = parser.get('BUFFER', profile)

    client = BackendApplicationClient(client_id=client_id)
    oauth = OAuth2Session(token=buffer_access, client=client)
    auth_code = oauth.token
    if profile == 'twitter':
        requests.post("https://api.bufferapp.com/1/updates/create.json?access_token=" + auth_code,
                      data={'profile_ids': buffer_profile,
                            'text': content + ' ' + url,
                            'now': 1})
    if profile == 'facebook':
        requests.post("https://api.bufferapp.com/1/updates/create.json?access_token=" + auth_code,
                      data={'profile_ids': buffer_profile,
                            'text': content + ' ' + url,
                            'link': url,
                            'now': 1})


def openjson(url):
    reader = codecs.getreader("utf-8")

    page_data = None
    try:
        api_request = Request(url)
        api_response = urlopen(api_request)

        try:
            page_data = json.load(reader(api_response))
        except (ValueError, KeyError, TypeError):
            page_data = "JSON error"

    except IOError as e:
        if hasattr(e, 'code'):
            page_data = e.code
        elif hasattr(e, 'reason'):
            page_data = e.reason
    return page_data


def process_pagedata(json_data):
    i = 0
    cursor.execute("SELECT CAST(ifnull(MAX(PublishDate),'2000-01-01') AS DATE) FROM WebsitePages")
    maxDate = time.strptime(str(cursor.fetchall()[0][0]), '%Y-%m-%d')

    for page in json_data:

        page_title = html.unescape(json_data[i]["title"]["rendered"])

        page_type = str(json_data[i]["categories"])
        page_type = page_type[1:(len(page_type) - 1)]

        try:
            c = page_type.index(",")
        except ValueError:
            c = -1

        if c == -1:
            page_type1 = int(page_type)
            page_type2 = None
        else:
            page_type1 = int(page_type[:c])
            page_type2 = int(page_type[(c + 1):])

        page_author = json_data[i]["cmb2"]["articles_metabox"]["_cmb_author"]
        page_url = json_data[i]["link"]

        page_date = json_data[i]["date"]

        page_desc = page_desc_format(pagedata[i]["cmb2"]['post_metabox']['_cmb_short_desc'])

        page_fb = ''
        page_tweet = ''

        if pagedata[i]["cmb2"]['post_metabox']['_cmb_facebook_post'] != '':
            page_fb = pagedata[i]["cmb2"]['post_metabox']['_cmb_facebook_post']
        else:
            page_fb = page_desc_format(pagedata[i]["cmb2"]['post_metabox']['_cmb_short_desc'])

        if pagedata[i]["cmb2"]['post_metabox']['_cmb_tweet'] != '':
            page_tweet = pagedata[i]["cmb2"]['post_metabox']['_cmb_tweet']
        else:
            page_tweet = page_title

        if time.strptime(page_date[:10], '%Y-%m-%d') > maxDate:
            cursor.execute("""INSERT INTO WebsitePages ( PageTitle,
                                                        PageTypePrimary,
                                                        PageTypeSecondary,
                                                        Author,
                                                        URL,
                                                        PageDesc,
                                                        TweetText,
                                                        FacebookText,
                                                        PublishDate)
                              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                           [page_title, page_type1, page_type2, page_author, page_url, page_desc, page_tweet, page_fb,
                            page_date])
            conn.commit()

            if page_type1 != 828 and page_type2 != 828:
                buffer_post('facebook', page_fb, page_url)
                cursor.execute("""INSERT INTO WebsiteSocialMediaHistory (   PageID            ,
                                                                            PageTitle		  ,
                                                                            PageTypePrimary	  ,
                                                                            PageTypeSecondary ,
                                                                            PublishDate	      ,
                                                                            PostingDate		  ,
                                                                            NovaraArchive	  ,
                                                                            Platform          ,
                                                                            PostType
                                                                        )
                                  SELECT MAX(ID), %s, %s, %s, %s, %s, 0, 'Facebook', 'InitialPublish'
                                  FROM  WebsitePages""",
                               [page_title, page_type1, page_type2, page_date, page_date])
                conn.commit()

                buffer_post('twitter', page_tweet, page_url)
                cursor.execute("""INSERT INTO WebsiteSocialMediaHistory (   PageID            ,
                                                                            PageTitle		  ,
                                                                            PageTypePrimary	  ,
                                                                            PageTypeSecondary ,
                                                                            PublishDate	      ,
                                                                            PostingDate		  ,
                                                                            NovaraArchive	  ,
                                                                            Platform          ,
                                                                            PostType
                                                                        )
                                  SELECT MAX(ID), %s, %s, %s, %s, %s, 0, 'Twitter', 'InitialPublish'
                                  FROM  WebsitePages""",
                               [page_title, page_type1, page_type2, page_date, page_date])
                conn.commit()

        i += 1


def page_desc_format(desc):
    index = 0
    string2 = None
    index = desc.find('<', index)
    while index != len(desc):
        if index != -1:
            string2 = desc[:index]
            index2 = desc.find('>', index)
            string2 += desc[index2 + 1:]
            desc = string2
            index = desc.find('<', index)
        else:
            if string2 is not None:
                return string2
            else:
                return desc


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

pagedata = openjson('http://novaramedia.com/wp-json/wp/v2/posts?per_page=10&page=1')

process_pagedata(pagedata)
