import pandas as pd
from owlready2 import *
import rdflib
from elasticsearch import Elasticsearch
from setting import config_read
from utils import change_prefix

# Read config file
config = config_read('../')

# Load owl file
data_path = config['owl']['path']
onto = get_ontology(data_path).load()

# Connect to Elasticsearch Server
server_ip = config['elasticsearch']['ip']
index_name = config['elasticsearch']['name']
es = Elasticsearch(server_ip)

###################################  Index Generation  ###################################


need_index = True

if need_index:
    # Create index on dataframe
    index_list = []
    index_list.extend([(change_prefix(data_path, r), "T_c", r.label) for r in onto.classes()])
    index_list.extend([(change_prefix(data_path, r), "T_op", r.label) for r in onto.object_properties()])
    index_list.extend([(change_prefix(data_path, r), "T_dp", r.label) for r in onto.data_properties()])

    instance_list = []
    for r in onto.individuals():
        if r.is_a[0] == Thing:  continue
        tbox = [change_prefix(data_path, t) for t in r.is_a]
        instance_list.append((change_prefix(data_path, r), "T_i", r.label, tbox))

    # Delete index on elasticsearch server
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name,ignore=[400, 404])
        print('Index has been deleted successfully')

    # Create index on elasticsearch server
    doc = {'URI':'owl:Thing', 'Type':'T_c'}
    es.index(index=index_name, body=doc)

    for row in index_list:
        doc = {'URI':row[0], 'Type':row[1], 'Annotation Values':row[2]}
        es.index(index=index_name, body=doc)

    for row in instance_list:
        doc = {'URI':row[0], 'Type':row[1], 'Annotation Values':row[2], 'Tbox':row[3]}
        es.index(index=index_name, body=doc)


#################################  Unit Path Extraction  #################################


need_unit_path = True

if need_unit_path:
    config = config_read('../')

    # Load owl file
    data_path = config['owl']['path']
    onto = get_ontology(data_path).load()

    g = rdflib.Graph()
    g.parse(data_path)

    knows_query = """
    SELECT DISTINCT ?x ?y ?z
    WHERE {
        ?x ?y ?z.
    }"""

    triple_list = []
    qres = g.query(knows_query)
    for row in qres:
        triple_list.append([str(row[0]), str(row[1]), str(row[2])])

    triple_text_list = []
    for s,p,o in triple_list:
        s_res = onto.search_one(iri=s)
        if s_res == None : 
            s_triple = s
        else:
            s_triple = s_res

        p_res = onto.search_one(iri=p)
        if p_res == None :
            p_triple = p
        else:
            p_triple = p_res

        o_res = onto.search_one(iri=o)
        if o_res == None : 
            o_triple = o
        else:
            o_triple = o_res
        
        triple_text_list.append([s_triple, p_triple, o_triple])

    triple_df = pd.DataFrame(triple_text_list, columns=['S','P','O'])
    triple_df = triple_df.drop_duplicates(['S','P','O']).reset_index(drop=True)

    p_list = list(onto.object_properties())
    p_list.extend(onto.data_properties())
    drop_prop_df = triple_df[triple_df.P.isin(p_list)].reset_index(drop=True)
    
    p_dict= {}
    for p in onto.data_properties():
        if len(p.range) == 0:
            continue
        ## Literal인 경우
        if p.range[0] == None:
            p_dict[p] = 'Literal_'+str(p)

        ## 다른 type인 경우
        elif hasattr(p.range[0], '__name__'):
            p_dict[p] = p.range[0].__name__+'_'+str(p)
        else:
            p_dict[p] = str(p.range[0])+'_'+str(p)
    

    range_tbox = []
    for i, row in drop_prop_df.iterrows():
        s,p,o = row
        # datatype property인 경우
        if p in p_dict:
            range_tbox.append(p_dict[p])
        # object property인 경우
        else:
            range_tbox.append(o.is_a)

    tbox_df = drop_prop_df.copy()

    tbox_df['domain'] = tbox_df['S'].apply(lambda x:x.is_a)
    tbox_df['range'] = range_tbox

    tbox_df = tbox_df.explode(['domain'])
    tbox_df = tbox_df.explode(['range'])

    tbox_df = tbox_df[tbox_df['domain'] != Thing]
    tbox_df = tbox_df.astype(str).reset_index(drop=True)

    # the number of instance-level triples containing the property of the unit path
    dpr_df = tbox_df.groupby(['domain', 'P', 'range'], as_index=False).S.count()

    # the total number of triples from domain class to range class
    dr_df = tbox_df.groupby(['domain', 'range'], as_index=False).S.count()

    weight_df = pd.merge(tbox_df, dpr_df, on=['domain', 'P', 'range'], how='inner', suffixes=('','_dpr'))
    weight_df = pd.merge(weight_df, dr_df, on=['domain', 'range'], how='inner', suffixes=('','_dr'))
    weight_df['W'] = 1 - (weight_df['S_dpr'] / weight_df['S_dr'])

    def subclass(tree):
        global head
        if type(tree) is not list:
            head = tree
            tree = list(head.subclasses())
        for node in tree :
            subclass_paths.append((node, 'rdfs.subClassOf', head, 0.0))
            subclass(list(node.subclasses()))
    
    subclass_paths = []
    classes = list(onto.classes())
    classes.append(Thing)
    for c in classes:
        subclass(c)
    subclass_df = pd.DataFrame(subclass_paths, columns=['domain', 'P', 'range', 'W'])


    subclass_df = pd.concat([weight_df[['domain', 'P', 'range', 'W']], subclass_df])
    subclass_df = subclass_df.drop_duplicates().reset_index(drop=True)
    

    final_list = []
    for i, row in subclass_df.iterrows():
        s,p,o,w = row
        final_list.append((change_prefix(data_path, s), change_prefix(data_path, p), change_prefix(data_path, o), w))

    final_df = pd.DataFrame(final_list, columns=['domain', 'P', 'range', 'W'])
    final_df.to_csv('../unit_path.csv', index=False)