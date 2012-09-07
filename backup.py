import urllib
import sqlite3
import json
import re
from datetime import datetime

def create_tables(conn):
  conn.execute('''create table IF NOT EXISTS message (
    id text PRIMARY KEY,
    created_at text,
    rawid text,
    text text,
    source text,
    truncated text,
    in_reply_to_status_id text,
    in_reply_to_user_id text,
    favorited text,
    in_reply_to_screen_name text,
    is_self text,
    location text,
    repost_status_id text,
    repost_user_id text,
    repost_screen_name text,
    photo text,
    user text
    )''')

  conn.execute('''create table IF NOT EXISTS user (
    id text PRIMARY KEY,
    name text,
    screen_name text,
    location text,
    gender text,
    birthday text,
    description text,
    profile_image_url text,
    profile_image_url_large text,
    url text,
    protected text,
    followers_count text,
    friends_count text,
    favourites_count text,
    statuses_count text,
    following text,
    notifications text,
    created_at text,
    utc_offset text,
    profile_background_color text,
    profile_text_color text,
    profile_link_color text,
    profile_sidebar_fill_color text,
    profile_sidebar_border_color text,
    profile_background_image_url text,
    profile_background_tile text
    )''')
  conn.commit()
  return conn

def save(conn, sql, keys, data):
  keys = re.split(r'\s*,\s*', keys)
  markers = []
  values = []
  for key in keys:
    if data.has_key(key):
      values.append(data[key])
    else:
      values.append('')
    markers.append('?')
  sql = sql % (','.join(keys), ','.join(markers))
  conn.execute(sql, tuple(values))

def save_user(conn, user):
  save(conn, '''insert into user (%s) values(%s)''', 
    '''id,
    name,
    screen_name,
    location,
    gender,
    birthday,
    description,
    profile_image_url,
    profile_image_url_large,
    url,
    protected,
    followers_count,
    friends_count,
    favourites_count,
    statuses_count,
    following,
    notifications,
    created_at,
    utc_offset,
    profile_background_color,
    profile_text_color,
    profile_link_color,
    profile_sidebar_fill_color,
    profile_sidebar_border_color,
    profile_background_image_url,
    profile_background_tile''', user)


def save_message(conn, message):

  #save user
  user = message['user'].copy()
  if user.has_key('created_at'):
    user['created_at'] = time(user['created_at'])
  try:
    save_user(conn, user)
  except sqlite3.IntegrityError,x:
    pass

  #convert message data
  message['user'] = user['id']
  if message.has_key('created_at'):
    message['created_at'] = time(message['created_at'])

  if message.has_key('photo'):
    message['photo'] = json.dumps(message['photo'])

  try:
    save(conn, '''insert into message (%s) values(%s)''',
      '''id,
      created_at,
      rawid,
      text,
      source,
      truncated,
      in_reply_to_status_id,
      in_reply_to_user_id,
      favorited,
      in_reply_to_screen_name,
      is_self,
      location,
      repost_status_id,
      repost_user_id,
      repost_screen_name,
      photo,
      user''', message)
  except sqlite3.IntegrityError, x:
    print x, message['id']
  conn.commit()

  #if message.has_key('repost_status'):
    #save_message(conn, message['repost_status'])

def fetch(opener, max_id=None):
  url = 'http://api.fanfou.com/statuses/user_timeline.json'
  data = {'count':60}
  if max_id is not None:
    data['max_id'] = max_id
  params = urllib.urlencode(data)
  url += '?' + params
  f = opener.open(url)
  return json.loads(f.read())

def time(ctime):
  return datetime.strptime(ctime, "%a %b %d %H:%M:%S +0000 %Y")

if __name__ == '__main__':

  conn = sqlite3.connect('backup.sqlite')

  create_tables(conn)

  opener = urllib.FancyURLopener()

  max_id = None
  
  while True:
    messages = fetch(opener, max_id)
    for message in messages:
      save_message(conn, message)
      max_id = message['id']
    if len(messages) < 60:
      break



