from stopwords import stopwords

import spacy
import logging
import re

allowed_entities = ['PERSON', 'FAC', 'ORG', 'GPE', 'LOC', 'PRODUCT', 'EVENT', '']
# MONEY


class Tokenizer:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.debug("loading spacy model")
        self.nlp = spacy.load('en', parser=False)
        self.logger.debug("model loaded")

    def tokenize(self, text, allow_urls=False, allow_stop=False):
        text_ht = re.sub(r'#(\w+)', r'ZZZPLACEHOLDERZZZ\1', text)
        doc = self.nlp(text_ht)
        tokens = []
        for token in doc:
            if not allow_stop and token.is_stop \
                    or token.lemma_.lower() in stopwords \
                    or not allow_urls and token.like_url \
                    or token.pos_ == 'PUNCT' \
                    or token.is_punct \
                    or token.text.startswith(('@', 'ZZZPLACEHOLDERZZZ')) \
                    or token.ent_type_ not in allowed_entities:
                if tokens:
                    tmp = tokens
                    tokens = []
                    yield tmp
                continue

            else:
                if token.ent_iob in (1, 3):
                    tokens.append(token)
                else:
                    if tokens:
                        tmp = tokens
                        tokens = []
                        yield tmp
                    else:
                        yield [token]

        if tokens:
            yield tokens


# t = Tokenizer()
# texts = [ #'The White House and his president, Obama Barack are going to the iPod release in Apple in Spanish http://www.google.com/',
#          '“The average Republican doesn’t even know what’s in that legislation”: Bernie Sanders blasts GOP on health care https://t.co/PFOGc1AxVL',
#          'Watch Marvel Director Taika Waititi’s clever new anti-racism PSA: https://t.co/2EB33RbvFG https://t.co/zdtXmAIyrP',
#          'Marvel, Disney and Fox are uniting in $1 billion deal.']
#
# tokens = list()
# for text in texts:
#     print(text)
#     for tt in t.tokenize(text):
#         print(tt)
#         tokens.append(tt)
#     print()

"""
def filter_tokens(tokens):
    for token in tokens:
        token = token.lower()

        if token in stopwords.words('english'):
            continue
        if token in string.punctuation:
            continue
        if re.findall(r'[^\w]+', token):
            continue
        if re.findall(r'(https?://t.co/[a-zA-Z0-9]+)', token):
            continue
        if token.startswith(('@', '#')):
            continue
        if len(token) <= 2:
            continue
        if re.findall(r'[0-9]+', token):
            continue

        yield token


def clean_tokenize(text):
    return filter_tokens(tknzr.tokenize(text))
"""