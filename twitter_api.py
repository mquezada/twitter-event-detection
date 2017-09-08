from db_api import get_saver, SessionPool
import tweepy
import logging
from twitter_keys import keys
import time


logger = logging.getLogger(__name__)


def get_api(key):
    auth = tweepy.OAuthHandler(key['consumer_key'], key['consumer_secret'])
    auth.set_access_token(key['access_token'], key['access_token_secret'])

    api = tweepy.API(auth)
    return api


def get_user(screen_names, api):
    for screen_name in screen_names:
        user = api.get_user(screen_name)
        yield user


def get_latest_tweets(screen_name, since_timestamp, api):
    # get latest tweets (max api count is 200)
    try:
        tweets = api.user_timeline(screen_name=screen_name, count=200)
    except tweepy.error.TweepError:
        logger.warning(f"User {screen_name} does not exist.")
        return None
    for tweet in tweets:
        if tweet.created_at >= since_timestamp:
            yield tweet


def is_retweet(tweet):
    return bool(getattr(tweet, 'retweeted_status', None))


class MyStreamListener(tweepy.StreamListener):
    def __init__(self, saver, stream_id, keywords, event_id, limit):
        super().__init__()
        self.saver = saver
        self.saver.send(None)

        self.start = time.time()
        self.limit = limit
        self.id = stream_id
        self.keywords = keywords
        self.event_id = event_id
        logger.info(f"Stream {self.id}. Keywords: <{keywords}>.")

    def on_status(self, status):
        if time.time() - self.start >= self.limit:
            logger.info(f"(Stream {self.id}) Timeout.")
            self.saver.close()
            return False

        logger.debug('"' + ' '.join(status.text.split()) + '"')
        self.saver.send((status, False, self.event_id))

    def on_error(self, status_code):
        logger.error(f'(Stream {self.id}) Error. Status code: {status_code}')
        if status_code == 420:
            return False

    def on_disconnect(self, notice):
        logger.error(f"(Stream {self.id}) Disconnected. Notice: {notice}")

    def on_exception(self, exception):
        logger.error(f"(Stream {self.id}) Exception: {exception}")

    def on_warning(self, notice):
        logger.warning(f"(Stream {self.id}) Warning: {notice}")

    def on_timeout(self):
        logger.error(f"(Stream {self.id}) Timeout.")


def collect_tweets(keyword_event_sets, limit=60*60):
    assert len(keyword_event_sets) <= len(keys)

    session_pool = SessionPool()
    streams = []

    for i, (keywords, event_id) in enumerate(keyword_event_sets):
        logger.info(f"Starting stream {i}")

        session = session_pool.get_session()
        saver = get_saver(session, buffer_size=4096)
        stream_listener = MyStreamListener(saver, stream_id=i, keywords=keywords, event_id=event_id, limit=limit)

        key = keys[i]
        auth = tweepy.OAuthHandler(key['consumer_key'], key['consumer_secret'])
        auth.set_access_token(key['access_token'], key['access_token_secret'])

        stream = tweepy.Stream(auth=auth, listener=stream_listener)
        stream.filter(track=[keywords], async=True, languages=['en'])
        streams.append(stream._thread)

    for stream in streams:
        stream.join()


if __name__ == '__main__':
    keyword_sets_ = [['covfefe', 1]]

    logging.basicConfig(format='%(asctime)s - %(name)s : %(message)s',
                        level=logging.INFO,
                        datefmt='%d-%m-%Y %H:%M:%S (%Z)')

    collect_tweets(keyword_sets_, limit=60)
    """
    since_timestamp = datetime.utcnow() - timedelta(hours=1)
    api = get_api(k0)

    tweets = get_latest_tweets('waxkun', since_timestamp, api)
    t = list(tweets)[0]
    """