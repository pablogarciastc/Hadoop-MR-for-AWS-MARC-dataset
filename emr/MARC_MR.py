#!/usr/bin/python

from mrjob.job import MRJob
from mrjob.step import MRStep
import simplejson as json


class Count(MRJob):
    def mapper(self, _, line):
        review = json.loads(line)
        stars = review["stars"]
        language = review["language"]
        product_id = review["product_id"]
        product_category = review["product_category"]

        key = {
            "stars": stars,
            "language": language,
            "product_category": product_id

        }
        key = json.dumps(key)

        yield(product_category, key)
        if (int(stars) == 1 or int(stars) == 5):
            # G from global
            yield("GS" + stars, 1)
        yield("GL" + language, 1)
        # yield("GLOBAL: " + product_category, 1)

    def reducer(self, word, counts):
        if word.startswith("G"):
            yield(word, sum(counts))
        else:
            counts = json.dumps(
                counts, iterable_as_array=True, ensure_ascii=True)
            counts = json.loads(counts)
            yield(word, counts)

    def reducer2(self, word, counts):
        if word.startswith("G"):
            if "1" in word:
                yield(word, sum(counts))
            elif "5" in word:
                yield(word, sum(counts))
            else:
                yield(word, sum(counts))
        else:
            counts = json.dumps(
                counts, iterable_as_array=True, ensure_ascii=True)
            counts = json.loads(counts)
            stars = 0
            counter = 0
            lista = []
            dict = {"en": 0, "es": 0, "fr": 0, "zh": 0, "ja": 0, "de": 0}
            for count in counts:
                for coun in count:
                    coun = json.loads(coun)
                    dict[coun["language"]] = dict[coun["language"]] + 1
                    stars = stars + int(coun["stars"])
                    counter = counter + 1
                    if coun["product_category"] not in lista:
                        lista.append(coun["product_category"])
            key = {
                "Average number of stars: ": str((stars/counter)),
                "Language with maximum reviews: ":  max(dict, key=dict.get),
                "Number of different products: ": str(len(lista)),
		"Number of products per category: ": str(counter)
            }
            yield("CT" + word, key)

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer
            ),
            MRStep(
                reducer=self.reducer2

            )
        ]


def output_process(x):
    stats = {}
    stats["StatisticsReviewsCorpus"] = {}
    stats["StatisticsReviewsCorpus"]["General"] = {}
    stats["StatisticsReviewsCorpus"]["General"]["Total number of reviews per language"] = {}
    stats["StatisticsReviewsCorpus"]["General"]["Total number of reviews with 1 star"] = {}
    stats["StatisticsReviewsCorpus"]["General"]["Total number of reviews with 5 stars"] = {}

    for key, value in x:
        if key.startswith("GL"):
            stats["StatisticsReviewsCorpus"]["General"]["Total number of reviews per language"][key[-2:]] = value
        if key.startswith("GS1"):
            stats["StatisticsReviewsCorpus"]["General"]["Total number of reviews with 1 star"] = value
        if key.startswith("GS5"):
            stats["StatisticsReviewsCorpus"]["General"]["Total number of reviews with 5 stars"] = value
        elif key.startswith("CT"):
            stats["StatisticsReviewsCorpus"][key[2:]] = value
    return stats


if __name__ == '__main__':
    Count().run()

