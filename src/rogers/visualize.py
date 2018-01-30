""" Utilities for visualization in Jupyter Notebook
"""
import numpy as np
import plotly.offline as py
from plotly.graph_objs import *
import networkx as nx
from textwrap import wrap

# TODO: fix normalize
np.seterr(divide='ignore', invalid='ignore')


def plt_neighbor_graph(query_samples_results, normalize=True, normalize_scale=100):
    """ Plot neighbor graph and add context in node info
    :param query_samples_results: output of index.query_samples
    :param normalize:
    :param normalize_scale:
    :return:
    """
    neighbors = []
    context = {}
    for results in query_samples_results:
        context[results['query'].sha256] = results['query'].contextual_features()
        for nbr in results['neighbors']:
            context[nbr[0].sha256] = nbr[0].contextual_features()
            neighbors.append({'query_hashval': results['query'].sha256,
                              'neighbor_hashval': nbr[0].sha256,
                              'similarity': nbr[1]})

    xs = np.array([n['similarity'] for n in neighbors])
    if normalize:
        xs_scale = np.full(len(xs), normalize_scale)
        idx = np.nonzero(xs - xs.min())
        min_idx = np.nonzero(xs == xs.min())
        xs_scale[min_idx] = 1
        if (xs.max() - xs.min()) > 0:
            xs_scale[idx] = ((xs[idx] - xs.min()) * normalize_scale) / (xs.max() - xs.min())
        layout_text = "Layout based on normalized similarity from 0 to %s" % normalize_scale
    else:
        xs_scale = xs
        layout_text = "Layout is not normalized"
    G = nx.Graph()
    hashvals = set([n['query_hashval'] for n in neighbors])

    for h in hashvals:
        G.add_node(h)

    for i, n in enumerate(neighbors):
        a = n['query_hashval']
        b = n['neighbor_hashval']
        similarity = n['similarity']
        G.add_edge(a, b, weight=xs_scale[i], similarity=similarity)

    pos = nx.spring_layout(G)
    for n_key in pos:
        G.nodes[n_key]['pos'] = pos[n_key]

    edge_trace = Scatter(
        x=[],
        y=[],
        line=Line(width=1.0, color='#888'),
        hoverinfo='text',
        text=[],
        mode='lines')

    edge_node_trace = Scatter(
        x=[],
        y=[],
        text=[],
        mode='markers',
        hoverinfo='text',
        marker=Marker(
            opacity=0
        )
    )

    for edge in G.edges(data=True):
        x0, y0 = G.node[edge[0]]['pos']
        x1, y1 = G.node[edge[1]]['pos']
        edge_trace['x'] += [x0, x1, None]
        edge_trace['y'] += [y0, y1, None]
        edge_node_trace['x'].append((x0 + x1) / 2)
        edge_node_trace['y'].append((y0 + y1) / 2)
        edge_node_info = "similarity: %s, weight: %s" % (str(edge[2]['similarity']), str(edge[2]['weight']))
        edge_node_trace['text'].append(edge_node_info)

    node_trace = Scatter(
        x=[],
        y=[],
        text=[],
        mode='markers',
        hoverinfo='text',
        marker=Marker(
            showscale=True,
            colorscale='YIGnBu',
            reversescale=True,
            color=[],
            size=10,
            colorbar=dict(
                thickness=15,
                title='# of eighbors',
                xanchor='left',
                titleside='right'
            ),
            line=dict(width=2)))

    for node in G.nodes():
        x, y = G.node[node]['pos']
        node_trace['x'].append(x)
        node_trace['y'].append(y)

    for node, adjacencies in G.adjacency():
        node_trace['marker']['color'].append(len(adjacencies))
        node_info = node
        node_info += '<br>neighbors: ' + str(len(adjacencies))
        for context_key in context[node].keys():
            val = context[node][context_key]
            if isinstance(val, list):
                node_info += "<br><br>%s:<br>    " % context_key + "<br>".join(
                    wrap(" ".join([str(v) for v in val]), width=60))
            else:
                node_info += "<br><br>%s:<br>    " % context_key + "<br>".join(wrap(str(val), width=60))
        node_trace['text'].append(node_info)

    fig = Figure(data=Data([edge_trace, node_trace, edge_node_trace]),
                 layout=Layout(
                     title='<br>Nearest Neighbor Sample Graph',
                     titlefont=dict(size=16),
                     showlegend=False,
                     hovermode='closest',
                     margin=dict(b=20, l=5, r=5, t=40),
                     annotations=[dict(
                         text=layout_text,
                         showarrow=False,
                         xref="paper", yref="paper",
                         x=0.005, y=-0.002)],
                     xaxis=XAxis(showgrid=True, zeroline=False, showticklabels=False),
                     yaxis=YAxis(showgrid=True, zeroline=False, showticklabels=False)))

    py.iplot(fig)