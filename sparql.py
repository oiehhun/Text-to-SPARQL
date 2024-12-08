import rdflib
import itertools
from sentence_transformers import SentenceTransformer, util

from langchain.chat_models import ChatOpenAI
from langchain.prompts import (ChatPromptTemplate,FewShotChatMessagePromptTemplate)
from dotenv import load_dotenv
load_dotenv()


class SPAQLConverter:

    def __init__(self, config):
        self.g = rdflib.Graph()
        self.g.parse(config['owl']['path'])

        SKMO = rdflib.Namespace("http://www.sktelecom.com/skmo/")
        self.g.bind("skmo", SKMO)
        SCHEMA = rdflib.Namespace("https://schema.org/")
        self.g.bind("temp", SCHEMA)
        SCHEMA = rdflib.Namespace("http://schema.org/")
        self.g.bind("schema", SCHEMA)


    def process(self, query_graph):
        final_query_graph = self.select_query_graph(query_graph) 
        relation_triples = self.extract_relation_triples(final_query_graph)
        knows_query = self.generate_query(relation_triples)
        result = self.excute_query(knows_query)
        self.print_result(result)

    
    def select_query_graph(self, user_input, query_graph):
        query_graph_scroe = sorted(query_graph, key=lambda x: (len(x[1]), -x[0]))
        score_max = query_graph_scroe[0][0]
        candidates_query_graph = []
        for i in query_graph_scroe:
            if i[0] == score_max:
                candidates_query_graph.append(i[1])
            else:
                break
        
        if len(candidates_query_graph) == 1:
            target = [qg[2] for qg in query_graph if qg[1] == candidates_query_graph[0]]
            return candidates_query_graph[0], target[0]
        else:
            final_query_graph = {}

            for query in candidates_query_graph:
                # formatted_query = ' '.join([s.split(':')[-1] for triple in query for s in triple])
                # formatted_query = formatted_query.replace(')', '')
                # formatted_query = ' '.join([s[s.find(':')+1:] for triple in query for s in triple])
                formatted_query = [s[s.find(':')+1:] for triple in query for s in triple]
                formatted_query = ' '.join([f[:f.find('(')+1] + f[f.find(':')+1:] if ':' in f else f for f in formatted_query])
                final_query_graph[formatted_query] = query
                
            sentence_sim = []

            for sentence2 in final_query_graph.keys():
                sentence1_representation = self.model.encode(user_input)
                sentence2_representation = self.model.encode(sentence2)

                cosine_sim = util.pytorch_cos_sim(sentence1_representation, sentence2_representation)
                print(f'{user_input}\n{sentence2}\n : {cosine_sim}')
                sentence_sim.append([cosine_sim, sentence2])
                print()
            
            x = sorted(sentence_sim, key=lambda x: -x[0])[0][1]
            target = [qg[2] for qg in query_graph if qg[1] == final_query_graph[x]]
            return final_query_graph[x], target[0]
    

    def extract_relation_triples(self, final_query_graph):
        relation_triples = []

        for s,p,o in final_query_graph:
            if p == 'rdfs:subClassOf': continue
            if (s,p,o) in relation_triples: continue
            
            relation_triples.append((s,p,o))
        return relation_triples

    
    def generate_query(self, relation_triples):
        target = self.set_target(relation_triples)
        knows_query = "SELECT DISTINCT ?target \nWHERE\n"
        prev_s = ''
        var_dict = {target:'?target'}
        c = itertools.count()

        knows_query += '{'


        for s,p,o in relation_triples:
            # domain, range 모두 instance 일때
            if s[-1] == ')' and o[-1] == ')':
                continue

            # domain이 instance 일때
            if s[-1] == ')':
                s_var = s[:-1].split('(')[-1]
            # domain이 class 일때
            else:
                if s in var_dict:
                    s_var = var_dict[s]
                else:
                    s_var = f'?x{next(c)}'
                    var_dict[s] = s_var

            # range가 instance 일때
            if o[-1] == ')':
                o_var = o[:-1].split('(')[-1]
            # range가 class 일때
            else:
                if o in var_dict:
                    o_var = var_dict[o]
                else:
                    o_var = f'?x{next(c)}'
                    var_dict[o] = o_var

            if prev_s == s_var:
                s_var = ';'
            else:
                if not prev_s == '':
                    knows_query += '.'
                prev_s = s_var
                knows_query += '\n    '
            
            knows_query += (' ').join([s_var, p, o_var])
            
        knows_query += '\n}'

        return knows_query
    

    def excute_query(self, knows_query):
        return self.g.query(knows_query)
    

    def set_target(self, relation_triples):
        if relation_triples[-1][-1][-1] == ')':
            target = relation_triples[-1][0]
        else:
            target = relation_triples[-1][-1]
        
        return target
    
    def print_result(self, result):
        for r in result:
            print(r['target'].rsplit('/')[-1], end='  ')
            
class ChatGenerator:
    
    def __init__(self) -> None:
        self.chat = ChatOpenAI(temperature=0, model_name='gpt-3.5-turbo')
        self.example = [
            {
                "query": "로쿠커피는 무슨 동에 있어?",
                "relation": "행정동명, 시도명, 시군구명, 지번주소, 상권업종소분류명, 상권업종중분류명, 상권업종대분류명, 건물에속한, 건물을점유하고있는, 건물그룹명, 그룹구성요소, 지점명",
                "answer": "로쿠커피, 행정동"
            },
            {
                "query": "천일디앤아이는 어느동에 위치해 있어?",
                "relation": "행정동명, 시도명, 시군구명, 지번주소, 상권업종소분류명, 상권업종중분류명, 상권업종대분류명, 건물에속한, 건물을점유하고있는, 건물그룹명, 그룹구성요소, 지점명",
                "answer": "천일디앤아이, 행정동"
            },
            {
                "query": "누마루건축사사무소의 주소가 뭐야?",
                "relation": "행정동명, 시도명, 시군구명, 지번주소, 상권업종소분류명, 상권업종중분류명, 상권업종대분류명, 건물에속한, 건물을점유하고있는, 건물그룹명, 그룹구성요소, 지점명",
                "answer": "누마루건축사사무소, 지번주소는"
            },
            {
                "query": "장안동본참치는 어떤 건물에 있어?",
                "relation": "행정동명, 시도명, 시군구명, 지번주소, 상권업종소분류명, 상권업종중분류명, 상권업종대분류명, 건물에속한, 건물을점유하고있는, 건물그룹명, 그룹구성요소, 지점명",
                "answer": "장안동본참치, 건물에속한"
            },
            {
                "query": "사고또사고 라는 곳은 무엇을 팔아?",
                "relation": "행정동명, 시도명, 시군구명, 지번주소, 상권업종소분류명, 상권업종중분류명, 상권업종대분류명, 건물에속한, 건물을점유하고있는, 건물그룹명, 그룹구성요소, 지점명",
                "answer": "사고또사고, 상권업종소분류명"
            }
        ]

        self.example_prompt = ChatPromptTemplate.from_messages(
            [
                ("human", "Query :{query}\n Relation : {relation}"),
                ("ai", "Entity Relation Extracted : {answer}")
            ]
        )

        self.few_shot_prompt = FewShotChatMessagePromptTemplate(
            example_prompt=self.example_prompt,
            examples=self.example,
            input_variables=["query", "relation"]
        )

        self.final_prompt = ChatPromptTemplate.from_messages(
            [
                ("system", "너의 역할은 Query와 Knowledge Graph내의 Relation이 주어졌을 때, Query 내에서 Entity와 Relatio이 될 수 있는 토큰을 추출하는 역할이야."),
                self.few_shot_prompt,
                ("human", "Query :{query}\n Relation : {relation}")
            ]
        )

        self.chain = self.final_prompt | self.chat

    def query_partition(self, query, relation):
        return self.chain.invoke({"query": query, "relation": relation})