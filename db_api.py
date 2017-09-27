import json
import logging

import arrow
from docopt import docopt
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import Base
from models import Tweet, User, EventTweet
from settings import settings

"""
database charset
see http://andy-carter.com/blog/saving-emoticons-unicode-from-twitter-to-a-mysql-database

SET NAMES utf8mb4;
ALTER DATABASE twitter_news CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
ALTER TABLE tweet_2017 CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
ALTER TABLE user_2017 CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
ALTER TABLE event_2017 CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

ALTER TABLE tweet_2017 CHANGE text text VARCHAR(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL;
ALTER TABLE user_2017 CHANGE name name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL;
ALTER TABLE user_2017 CHANGE description description VARCHAR(2048) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

"""


logger = logging.getLogger(__name__)
# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)


def format_date(date_str, fmt="ddd MMM DD HH:mm:ss Z YYYY"):
    # e.g. date_str == "Wed Aug 27 13:08:45 +0000 2008"
    return arrow.get(date_str, fmt).datetime


class SessionPool:
    def __init__(self):
        self.engine = create_engine(
            "mysql://{}:{}@{}/{}?charset=utf8mb4".format(
                settings["DB"]["user"],
                settings["DB"]["password"],
                settings["DB"]["host"],
                settings["DB"]["database"]
            ),
            isolation_level="READ_COMMITTED",
            pool_recycle=30
        )
        self.Session = sessionmaker(self.engine, autocommit=True)

    def get_session(self):
        return self.Session()


def get_saver(session, buffer_size=2**16):
    buffer = []
    closed = False

    while True:
        try:
            t, is_headline, event_id = (yield)

            if not t:
                continue

            quoted_status_id = None
            if t.is_quote_status:  # has quote
                if getattr(t, 'quoted_status', None):
                    qt, qu, et = parse_dict(t.quoted_status, event_id)
                    quoted_status_id = qt.id

                    buffer.append(qt)
                    buffer.append(qu)
                    buffer.append(et)

            retweeted_status_id = None
            if t.retweeted:  # is retweet
                if getattr(t, 'retweeted_status', None):
                    rt, ru, et = parse_dict(t.retweeted_status, event_id)
                    retweeted_status_id = t.retweeted_status.id

                    buffer.append(rt)
                    buffer.append(ru)
                    buffer.append(et)

            u = t.user

            entities = getattr(u, 'entities', None)
            if entities:
                entities = json.dumps(entities)

            user = User(user_id=u.id,
                        verified=u.verified,
                        name=u.name,
                        screen_name=u.screen_name,
                        created_at=u.created_at,
                        description=u.description,
                        location=u.location,
                        geo_enabled=u.geo_enabled,
                        entities=entities,
                        lang=u.lang,
                        url=u.url,
                        followers_count=u.followers_count,
                        favourites_count=u.favourites_count,
                        listed_count=u.listed_count,
                        friends_count=u.friends_count,
                        statuses_count=u.statuses_count,
                        utc_offset=u.utc_offset,
                        time_zone=u.time_zone)

            tweet = Tweet(tweet_id=t.id,
                          text=t.text,
                          created_at=t.created_at,
                          source=t.source,
                          source_url=t.source_url,
                          entities=json.dumps(t.entities),  # <- this is a dict
                          lang=t.lang,
                          truncated=t.truncated,
                          possibly_sensitive=getattr(t, 'possibly_sensitive', False),
                          coordinates=json.dumps(t.coordinates),  # <- dict?
                          in_reply_to_status_id=t.in_reply_to_status_id,
                          in_reply_to_screen_name=t.in_reply_to_screen_name,
                          in_reply_to_user_id=t.in_reply_to_user_id,
                          favorite_count=t.favorite_count,
                          retweet_count=t.retweet_count,
                          is_headline=is_headline,
                          quoted_status_id=quoted_status_id,
                          is_a_retweet=t.retweeted,
                          retweeted_status_id=retweeted_status_id,
                          user_id=t.user.id)

            if event_id:
                event_tweet = EventTweet(tweet_id=t.id,
                                         event_id=event_id)
                buffer.append(event_tweet)

            buffer.append(user)
            buffer.append(tweet)

        except GeneratorExit:
            closed = True
            break

        finally:
            if closed or len(buffer) >= buffer_size:
                added_tweets = set()
                added_users = set()

                with session.begin():
                    logger.info(f"Saving data... (buffer size: {len(buffer)})")
                    for obj in buffer:
                        if isinstance(obj, Tweet) and obj.tweet_id not in added_tweets:
                            added_tweets.add(obj.tweet_id)
                            session.add(obj)
                        elif isinstance(obj, User) and obj.user_id not in added_users:
                            added_users.add(obj.user_id)
                            session.add(obj)
                        elif isinstance(obj, EventTweet):
                            session.add(obj)

                buffer = []


def parse_dict(tweet_dict, event_id=None):
    t = tweet_dict
    u = t['user']

    tweet = Tweet(tweet_id=t['id'],
                  text=t['text'],
                  created_at=format_date(t['created_at']),
                  source=t['source'],
                  source_url=t['source'],
                  entities=json.dumps(t['entities']),  # <- this is a dict
                  lang=t['lang'],
                  truncated=t['truncated'],
                  possibly_sensitive=t.get('possibly_sensitive', False),
                  coordinates=json.dumps(t['coordinates']),  # <- dict?
                  in_reply_to_status_id=t['in_reply_to_status_id'],
                  in_reply_to_screen_name=t['in_reply_to_screen_name'],
                  in_reply_to_user_id=t['in_reply_to_user_id'],
                  favorite_count=t['favorite_count'],
                  retweet_count=t['retweet_count'],
                  is_headline=False,
                  quoted_status_id=t.get('quoted_status_id', None),
                  is_a_retweet=t.get,
                  retweeted_status_id=t.get('retweeted_status_id', None),
                  user_id=t['user']['id'])

    if event_id:
        event_tweet = EventTweet(tweet_id=t['id'], event_id=event_id)
    else:
        event_tweet = None

    if u.get('entities'):
        entities = json.dumps(u['entities'])
    else:
        entities = None

    user = User(user_id=u['id'],
                verified=u['verified'],
                name=u['name'],
                screen_name=u['screen_name'],
                created_at=format_date(u['created_at']),
                description=u['description'],
                location=u['location'],
                geo_enabled=u['geo_enabled'],
                entities=entities,
                lang=u['lang'],
                url=u['url'],
                followers_count=u['followers_count'],
                favourites_count=u['favourites_count'],
                listed_count=u['listed_count'],
                friends_count=u['friends_count'],
                statuses_count=u['statuses_count'],
                utc_offset=u['utc_offset'],
                time_zone=u['time_zone'])

    return tweet, user, event_tweet


################################################################


def create_tables():
    engine = create_engine(
        "mysql://{}:{}@{}/{}?charset=utf8mb4".format(
            settings["DB"]["user"],
            settings["DB"]["password"],
            settings["DB"]["host"],
            settings["DB"]["database"]
        ),
        isolation_level="READ_COMMITTED",
        pool_recycle=30
    )
    Base.metadata.create_all(bind=engine)


def main():
    doc = """
    Usage:
        db_api.py create_tables
    """
    docopt(doc)

    q = "Are you sure you want to create tables on database? (y/[n]) "
    ans = input(q)
    if ans == "y":
        create_tables()

if __name__ == '__main__':
    main()

