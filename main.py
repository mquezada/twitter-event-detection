"""News collector

Usage:
    main.py [--debug]
    main.py -h | --help

Options:
  --debug         Show debug log.
  -h --help       Show this screen.
"""

from settings import settings
from twitter_api import get_latest_tweets, get_api, is_retweet, collect_tweets
from twitter_keys import keys
from db_api import get_saver, SessionPool
from detect_keywords import detect_keywords
from docopt import docopt
from clean_tweets import Tokenizer
from models import Event

from contextlib import closing
from datetime import datetime, timedelta
import logging
import time

logger = logging.getLogger(__name__)
second = 1
minute = 60 * second
hour = 60 * minute


def main():
    # simultaneous crawlers
    n_threads = 6

    # load tokenizer
    tokenizer = Tokenizer()

    # load news account IDs
    news_accounts = settings['news_accounts']

    # load api
    api = get_api(keys[0])

    session = SessionPool().get_session()

    # for every hour:
    # Get headline tweets published from one hour ago
    headlines = []
    one_hour_before = datetime.utcnow() - timedelta(hours=1)

    with closing(get_saver(session)) as saver:
        saver.send(None)

        for screen_name in news_accounts:
            news_sources_tweets = get_latest_tweets(screen_name, one_hour_before, api)
            if not news_sources_tweets:
                continue
            for tweet in news_sources_tweets:
                text = tweet.text
                if is_retweet(tweet):
                    text = tweet.retweeted_status.text

                headlines.append(text)
                saver.send((tweet, True, None))

    headlines_preprocessed = []
    for headline in headlines:
        doc = set()
        for term in tokenizer.tokenize(headline):
            term = ' '.join([t.lower_ for t in term])
            doc.add(term)
        headlines_preprocessed.append(doc)

    keywords = detect_keywords(headlines_preprocessed, threshold=2)

    events = []
    keyword_sets = []

    with session.begin():
        # take n_threads first keyword sets (top score first)
        # for each keyword set, take first 3 keywords (random order)
        for kwd in keywords[:n_threads]:
            tmp = list(kwd[0])[:3]

            event = Event(keyword1=tmp[0],
                          keyword2=tmp[1],
                          keyword3=tmp[2] if len(tmp) == 3 else None)
            session.add(event)
            events.append(event)
            keyword_sets.append(' '.join(tmp))

    for event in events:
        session.refresh(event)

    keyword_sets = list(zip(keyword_sets, map(lambda x: x.id, events)))

    # collect tweet sets per keyword set for 1 hour
    collect_tweets(keyword_sets, limit=1 * minute)

    return keywords, headlines


if __name__ == "__main__":
    args = docopt(__doc__)

    if args['--debug']:
        logging.basicConfig(format='%(asctime)s - %(name)s : %(message)s',
                            level=logging.DEBUG,
                            datefmt='%d-%m-%Y %H:%M:%S %Z')
    else:
        logging.basicConfig(format='%(asctime)s - %(name)s : %(message)s',
                            level=logging.INFO,
                            datefmt='%d-%m-%Y %H:%M:%S %Z')
        logging.Formatter.converter = time.gmtime

    kwd, hdl = main()
