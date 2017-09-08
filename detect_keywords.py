import numpy as np
from operator import itemgetter
from collections import defaultdict
from itertools import product


def intersection_n(ha, hb, threshold=0):
    if ha == hb:
        return False

    return len(ha & hb) >= threshold


def detect_keywords(tokens_sets, threshold):
    """detect common keywords in headlines, based on supplementary material PLOS One"""
    h = tokens_sets
    candidates = list()
    scores = list()

    headlines_pairs = filter(lambda x: intersection_n(x[0], x[1], threshold), product(h, h))
    for ha, hb in headlines_pairs:
        g = ha & hb

        if not candidates:
            candidates.append(g)
            scores.append(defaultdict(int))
            for w in candidates[0]:
                scores[0][w] = 1

        j = np.argmax([len(candidate & g) for candidate in candidates])

        if len(candidates[j] & g) >= threshold:
            candidates[j] = candidates[j] & g
            for w in candidates[j]:
                scores[j][w] += 1
        else:
            candidates.append(g)
            scores.append(defaultdict(int))
            for w in candidates[-1]:
                scores[-1][w] = 1

    total_scores = [sum(score.values()) for score in scores]
    return sorted(zip(candidates, total_scores), key=itemgetter(1), reverse=True)
