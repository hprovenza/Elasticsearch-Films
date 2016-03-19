__author__ = 'hannahprovenza'


from elasticsearch import Elasticsearch, helpers
import json


"""
Define an elasticsearch schema (settings & mappings)
- Document type(s)
- Properties of document type(s)
- Filters, analyzers
"""

class build_es_movies_index():
    """
    Create the index
    - Initialize with index name and mapping
    - Load documents using the bulk function
    - Refresh index after loading
    """

    def __init__(self):
        '''
        Create an Elasticsearch object, delete any old version of the index, and initialize the index.
        '''
        self.es = Elasticsearch()
        self.INDEX_NAME = "es_movies_provenza"

        # Delete old index
        if self.es.indices.exists(self.INDEX_NAME):
            res = self.es.indices.delete(index = self.INDEX_NAME)

        #initialize mapping
        with open("es_schema.json") as data_file:
            schema = json.load(data_file)

        # create index
        res = self.es.indices.create(index=self.INDEX_NAME, body=schema)

        # load documents
        with open("2015_movies.json") as data_file:
            movies = json.load(data_file)
        self.bulk_insert(self.es, movies)



        #refresh index
     #   self.es.refresh(index=self.INDEX_NAME)
        print("Index build complete.")


    def format_action(self, id, value):
        '''
        :param id: the ID number of a film
        :param value: the data associated with a film
        :return: a dict representing the index information of this film
        '''
        return {
            "_index": self.INDEX_NAME,
            "_type": "movie",
            "_id": int(id),
            "_source": value
        }


    def bulk_insert(self, es, movies):
        '''
        :param es:
        :param movies:
        :return:
        '''
        actions = []
        for key, value in movies.iteritems():
            actions.append(self.format_action(key, value))
        return helpers.bulk(es, actions, stats_only=True)


    def q_total(self):
        """
        :return: the number of documents in the index
        """
        res = self.es.count(index=self.INDEX_NAME, body={"query": {"match_all": {}}})
        return res["count"]


    def q_time_range(self, year1, year2):
        """
        :param year1:  the lower bound of the date range
        :param year2: the upper bound of the date range
        :return: doc count with time within range from year1 to year 2 inclusive
        """
        res = self.es.count(index = self.INDEX_NAME, body=
        {
            "query": {
                "range": {
                    "time" : {
                        "gte": year1,
                        "lte": year2
                    }
                }
            }
        })
        return res["count"]


    def q_field(self, field_name, value):
        """
        :param field_name: the name of the field in which we hope to find a match
        :param value: the term for which we are searching
        :return" docs and their field values (for specified field)
        Search within specific fields (e.g., director, country): q_field(field_name, value)
        """
        if field_name in ["time", "runtime"]:
            res = self.es.search(index = self.INDEX_NAME, body=
            {
                "query": {
                    "match" : {
                        field_name : value
                    }
                }
            })
            return res
        res = self.es.search(index = self.INDEX_NAME, body=
        {
            "query": {
                "match" : {
                    field_name : value
                }
            }
        }, analyzer = "snowball_analyzer")
        return [movie["_source"]["title"] for movie in res["hits"]["hits"]]


    def q_mw(self, string):
        '''
        :param string: a set of words to be searched for in the title and text fields only
        Search text using multiword queries: q_mw(string)
        Match over title and text fields only
        '''
        res = self.es.search(index = self.INDEX_NAME, body=
        {
            "query": {
                "multi_match" : {
                    "query" : string,
                    "fields" : ["title", "text"]
                }
            }
        })
        return [film["_source"]["title"] for film in res["hits"]["hits"]]


    def q_phr(self, phrase):
        """
        :param phrase: a string of text to search for
        Search text using a phrase: q_phr(phrase)
        Test phrase match
        Compare to result for multiword query of same phrase
        """
        res = self.es.search(index = self.INDEX_NAME, body=
        {
            "query": {
                "multi_match" : {
                    "query" : phrase,
                    "fields" : ["title", "text"],
                    "type" : "phrase"
                }
            }
        })
        return [film["_source"]["title"] for film in res["hits"]["hits"]]


    def q_fs(self, *args):
        """
        :param args: field, value pairs
        Search on a combination of fields: q_fs([field, value]*)
        Enforce conjunctive queries (all terms within a field, except
        noisewords, and all fields must match)
        """
        res = self.q_field(args[0][0], args[0][1])
        for x in args:
            res = self.intersect(res, self.q_field(x[0], x[1]))
        return res

    def intersect(self, a, b):
        """
        Returns the intersection of two lists
        :param a: a list
        :param b: another list
        :return: the intersection of those lists
        """
        return [x for x in a if x in b]

def main():
    index = build_es_movies_index()
    print "Total number of results: " + str(index.q_total())
    print
    print "Movies set between 1990 and 2000: "
    print index.q_time_range(1990, 2000)
    print
    print "Films with \"time\" in the title:"
    print index.q_field("title", "time")
    print
    print "Films with \"once upon a time\" in the title or text (multiword search): "
    print index.q_mw("once upon a time")
    # Most of those words are stopwords, so really we're just searching for "time" here!
    print
    print "Films with phrase \"once upon a time\" in the title or text (phrase search)"
    print index.q_phr("once upon a time")
    # Because this is including the stopwords and their close proximity, we get a lot fewer hits here.
    print
    print "Films with \"time\" and \"robbery\" in the title:"
    print index.q_fs(("title", "time"), ("title", "robbery"))

if __name__ == "__main__":
    main()
