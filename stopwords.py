from nltk.corpus import stopwords

stopwords = stopwords.words('english')

# palabras antiguas del primer proceso
more_words = [
    "ain",
    "doesn",
    "isn",
    "don",
    "live",
    "like",
    "watch",
    "update",
    "updates",
    "breaking",
    "news",
    "report",
    "reports",
    "video",
    "day",
    "today",
    "tomorrow",
    "yesterday",
    "twitter",
    "says",
    "say",
    "front",
    "page",
    "via",
    "ndtv",
    "tweet",
    "tweets"
]

# palabras nuevas (2017)
more_more_words = [
    '’s',
    "'s",
    'bbcpapers'
]

stopwords += more_words + more_more_words
