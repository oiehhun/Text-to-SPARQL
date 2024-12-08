import numpy as np
import itertools
from owlready2 import get_ontology
from elasticsearch import Elasticsearch
from konlpy.tag import Okt

class ResourceMapper:

    def __init__(self, config):
        # Owlready2
        data_path = config['owl']['path']
        self.onto = get_ontology(data_path).load()

        # Elasticsearch
        server_ip = config['elasticsearch']['ip']
        self.index_name = config['elasticsearch']['name']
        self.es = Elasticsearch(server_ip)

        # OKT
        self.okt = Okt()



    def process(self, user_input=None):
        query_terms = self.tokenize_query(user_input)
        query_patitions = self.partition_query(query_terms)
        p_star,_ = self.score_partition(query_patitions)
        resource_combinations = self.combinate_resource(p_star)
        return resource_combinations
    


    def tokenize_query(self, user_input=None):
        if user_input == None:
            user_input = input("Question: ")
        query_pos = self.okt.pos(user_input, norm=True)
        query_terms = [q[0] for q in query_pos if q[1] not in ["Josa", "Punctuation"]]
        return query_terms



    def partition_query(self, query_terms):
        query_patitions = [[" ".join(query_terms)]]

        for slice_num in range(1,len(query_terms)):
            for slice_list in itertools.combinations(range(len(query_terms)-1), slice_num):
                qp = []
                qp.append(" ".join(query_terms[:slice_list[0]+1]))
                
                for s_i, slice_idx in enumerate(slice_list):
                    if s_i == len(slice_list) - 1 : break
                    qp.append(" ".join(query_terms[slice_idx+1:slice_list[s_i+1]+1]))

                qp.append(" ".join(query_terms[slice_list[-1]+1:]))
                query_patitions.append(qp)
        
        return query_patitions



    def score_partition(self, query_patitions):
        score_list = []

        for qp in query_patitions:
            score = 0
            for q in qp:
                search_query = {"query":{"term":{"Annotation Values.keyword": q}}}
                result = self.es.search(index=self.index_name, body=search_query)
                if result['hits']['max_score'] :
                    score += result['hits']['max_score']
            score_list.append(score)
        
        p_star = query_patitions[np.argmax(score_list)]

        return p_star, score_list



    def combinate_resource(self, p_star):
        candidates_list = []

        for token in p_star:
            search_query = {"query":{"term":{"Annotation Values.keyword": token}}}
            result = self.es.search(index=self.index_name, body=search_query)
            candidates = []
            for res in result['hits']['hits']:
                candidates.append(res['_source']['URI'])
            candidates_list.append(candidates)

        return list(itertools.product(*candidates_list))