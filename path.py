from heapq import heappop, heappush
import itertools
import networkx as nx


def generate_graph(unit_path):
    G = nx.MultiDiGraph()

    for _, row in unit_path.iterrows():
        source = row['domain']  
        target = row['range']   
        edge = row['p']         
        weight = row['W']       

        G.add_node(source)
        G.add_node(target)
        G.add_edge(source, target, property=edge, weight=weight, label=edge)
    
    return G


def find_shortest_path(G, source, target, p_name=None, n_triple=None, weight=False):
    push = heappush
    pop = heappop
    
    c = itertools.count()
    fringe = []
    path = [source]
    min_len = float('inf')
    result = []

    if n_triple is None:
        n_triple = len(G.nodes)

    if source == target:
        return min_len, result
    
    push(fringe, (0, next(c), source, path, 0))

    while fringe:
        (d, loop, v, path, g_score) = pop(fringe)

        if len(path)//2 >= n_triple:
            continue
    
        if target == path[-1]:
            if d > min_len: 
                break
            if p_name is None or p_name in path:
                result.append((g_score, path))
                min_len = d
            continue

        if d == min_len:
            continue

        for u, edges in G._adj[v].items():
            if u in path:
                continue
            for _, e in edges.items():
                
                if weight: cost = e['weight']
                else: cost = 1

                new_path = list()
                new_path += path
                new_path += [e['property'], u]

                push(fringe, (d+cost, next(c), u, new_path, g_score + e['weight']))

    if len(result) == 0:
        return min_len, result

    result = [(g, [tuple(r[offset:offset+3]) for offset in range(0, len(r)-2, 2)]) for (g, r) in result]
    return min_len, result