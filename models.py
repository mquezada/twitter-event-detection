from collections import namedtuple
from datetime import datetime

from sqlalchemy import Column, Integer, String, DateTime, BigInteger, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base

TWEET = 'tweet_2017'
EVENT = 'event_2017'
USER = 'user_2017'
EVENT_TWEET = 'event_tweet_2017'

tweet_tuple = namedtuple('tweet_tuple', ['tweet', 'is_headline', 'event_id'])
Base = declarative_base()


class EventTweet(Base):
    __tablename__ = EVENT_TWEET

    id = Column(Integer, primary_key=True)
    tweet_id = Column(BigInteger, index=True)
    event_id = Column(Integer, index=True)
    when_added = Column(DateTime, default=datetime.now)


class Event(Base):
    __tablename__ = EVENT

    id = Column(Integer, primary_key=True)
    keyword1 = Column(String(255), index=True)
    keyword2 = Column(String(255), index=True)
    keyword3 = Column(String(255), index=True)
    when_added = Column(DateTime, default=datetime.now, index=True)


class Tweet(Base):

    __tablename__ = TWEET

    id = Column(Integer, primary_key=True)
    tweet_id = Column(BigInteger, index=True)  # == id
    text = Column(String(1024))
    created_at = Column(DateTime)  # e.g. "Wed Aug 27 13:08:45 +0000 2008"

    when_added = Column(DateTime, default=datetime.now)

    source = Column(String(255))
    source_url = Column(String(1024))
    entities = Column(Text)
    lang = Column(String(255))
    truncated = Column(Boolean)
    possibly_sensitive = Column(Boolean)

    coordinates = Column(String(255))
    # place == Place

    in_reply_to_status_id = Column(BigInteger)
    in_reply_to_screen_name = Column(String(255))
    in_reply_to_user_id = Column(BigInteger)

    favorite_count = Column(Integer)
    retweet_count = Column(Integer)

    is_headline = Column(Boolean)

    quoted_status_id = Column(BigInteger, nullable=True)

    is_a_retweet = Column(Boolean)
    retweeted_status_id = Column(BigInteger, nullable=True)

    user_id = Column(BigInteger)


class User(Base):
    __tablename__ = USER

    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, index=True)  # id
    verified = Column(Boolean)

    when_added = Column(DateTime, default=datetime.now)

    name = Column(String(255))
    screen_name = Column(String(255), index=True)
    created_at = Column(DateTime)

    description = Column(String(2048))
    location = Column(String(512))
    geo_enabled = Column(Boolean)
    entities = Column(Text)
    lang = Column(String(255))
    url = Column(String(255))

    followers_count = Column(Integer)
    favourites_count = Column(Integer)
    listed_count = Column(Integer)
    friends_count = Column(Integer)  # followings
    statuses_count = Column(Integer)

    utc_offset = Column(String(255))
    time_zone = Column(String(255))
