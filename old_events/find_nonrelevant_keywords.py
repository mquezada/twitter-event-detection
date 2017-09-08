import numpy as np
import arrow
from collections import defaultdict
from tqdm import tqdm


events = []
with open('event_keywords.txt') as f:
    next(f)
    for line in f:
        events.append(line.split('\t'))


def tf(term, document):
    count = np.sum([1 for t in document if t == term])
    return count / len(document)


def idf(term, corpus):
    n = len(corpus)
    nt = np.sum([1 for doc in corpus if term in doc])

    return np.log(n / nt)


def tfidf(term, document, corpus):
    return tf(term, document) * idf(term, corpus)


corpus = defaultdict(list)
vocabulary = set()
for event in events:
    keywords = event[0].split()
    date = arrow.get(event[1], "YYYY-MM-DD").date()
    corpus[date].extend(keywords)

    vocabulary.add(keywords[0])
    vocabulary.add(keywords[1])

scores = defaultdict(lambda: dict())
corpus_ = corpus.values()
for i, document in tqdm(enumerate(corpus_), total=len(corpus_)):
    for term in document:
        scores[i][term] = tfidf(term, document, corpus_)

words = defaultdict(float)
for scores_dict in scores.values():
    sorted_scores = sorted(scores_dict.items(), key=lambda x: x[1])
    for word, score in sorted_scores[:5]:
        words[word] += score