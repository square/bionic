from collections import defaultdict

import pandas as pd


def visualize_dag_graphviz(dag):
    import networkx as nx

    child_lists_by_parent, _ = structure(dag)
    G = nx.DiGraph()
    G.add_edges_from([
        (parent, child)
        for parent, children in child_lists_by_parent.items()
        for child in children
    ])
    return nx.drawing.nx_pydot.to_pydot(G)


def visualize_dag_matplotlib(dag, figsize=None):
    child_lists_by_parent, clusters_by_node = structure(dag)
    return render_dag_tiers(child_lists_by_parent, clusters_by_node, figsize)


def structure(flow):
    names_by_task_key = {}
    clusters_by_node = {}
    flow._resolver.get_ready()
    for resource_name, tasks in (flow._resolver._task_lists_by_resource_name.items()):
        if flow._resource_is_core(resource_name):
            continue

        if len(tasks) == 1:
            name_template = '{resource_name}'
        else:
            name_template = '{resource_name}[{ix}]'

        for ix, task in enumerate(tasks):
            name = name_template.format(resource_name=resource_name, ix=ix)
            names_by_task_key[task.key] = name
            clusters_by_node[name] = resource_name

    child_lists_by_parent = {}
    for state in flow._resolver._task_states_by_key.values():
        if flow._resource_is_core(state.task.key.resource_name):
            continue
        name = names_by_task_key[state.task.key]

        child_names = list()
        for child_state in state.children:
            child_name = names_by_task_key[child_state.task.key]
            if flow._resource_is_core(child_name):
                continue
            child_names.append(child_name)

        child_lists_by_parent[name] = child_names
    return child_lists_by_parent, clusters_by_node


def toposorted(parent_lists_by_child):
    '''
    Topologically sort the nodes in a DAG.  If the DAG is passed as a map from
    child to parent-list, it is sorted from ancestors to descendants; if it is
    passed as a map from parent to child-list, it is sorted from descendants to
    ancestors.
    '''
    visited_nodes = set()
    sorted_nodes = []

    def sort_ancestors(node):
        if node in visited_nodes:
            return
        visited_nodes.add(node)

        for parent in parent_lists_by_child[node]:
            sort_ancestors(parent)

        sorted_nodes.append(node)

    for child in parent_lists_by_child.keys():
        sort_ancestors(child)

    return sorted_nodes


def invert_dict_into_multidict(values_by_key):
    '''
    Given a dict from keys to values, return the inverted dict (from values to
    lists of keys).
    '''
    key_lists_by_value = defaultdict(list)
    for key, value in values_by_key.items():
        key_lists_by_value[value].append(key)
    return dict(key_lists_by_value)


def node_heights(child_lists_by_parent):
    '''
    Given a DAG, return a map from each node to its "height": the length of the
    longest path starting at that node.
    '''
    heights_by_node = {}
    for node in toposorted(child_lists_by_parent):
        children = child_lists_by_parent[node]
        if len(children) == 0:
            height = 0
        else:
            height = 1 + max(heights_by_node[child] for child in children)
        heights_by_node[node] = height

    return heights_by_node


def render_dag_tiers(child_lists_by_parent, clusters_by_node, figsize=None):
    '''
    Plots a DAG using a simple rank-based layout algorithm.
    '''
    import networkx as nx
    from matplotlib import pyplot as plt

    if figsize is None:
        figsize = (12, 8)

    # Constants.
    node_size = 100
    between_tier_space = 100
    within_tier_space = 100
    within_cluster_space = 20

    between_tier_step = node_size + between_tier_space
    within_cluster_step = node_size + within_cluster_space

    # Identify the node clusters and the cluster graph connections.
    node_lists_by_cluster = invert_dict_into_multidict(clusters_by_node)

    cluster_children_by_parent = defaultdict(list)
    for cluster, member_nodes in node_lists_by_cluster.items():
        cluster_children = set()
        for member_node in member_nodes:
            for child_node in child_lists_by_parent[member_node]:
                child_cluster = clusters_by_node[child_node]
                cluster_children.add(child_cluster)
        cluster_children_by_parent[cluster] = list(cluster_children)

    # Calculate the "height" of each cluster.
    heights_by_cluster = node_heights(cluster_children_by_parent)
    cluster_lists_by_height = invert_dict_into_multidict(heights_by_cluster)

    # Iterate through each height level, from the the lowest to the highest.
    xs_by_node = {}
    ys_by_node = {}
    ys_by_cluster = {}
    for height, clusters in sorted(cluster_lists_by_height.items()):
        x = -between_tier_step * height

        # Attempt to align each cluster based on its children.
        cluster_ys = []
        cluster_spans = []
        for cluster in clusters:
            cluster_children = cluster_children_by_parent[cluster]
            if len(cluster_children) == 0:
                y = 0
            else:
                y = pd.Series(
                    ys_by_cluster[child] for child in cluster_children).mean()
            cluster_ys.append(y)
            ys_by_cluster[cluster] = y

            size = len(node_lists_by_cluster[cluster])
            span = (
                (node_size + within_cluster_space) * size -
                within_cluster_space)
            cluster_spans.append(span)

        # Space out the clusters.
        cdf = pd.DataFrame()
        cdf['name'] = clusters
        cdf['y'] = cluster_ys
        cdf['span'] = cluster_spans

        cdf['step'] = cdf['y'].diff()
        cdf['step_occupied'] = (cdf['span'] + cdf['span'].shift(1)) / 2
        cdf['step_empty'] = cdf['step'] - cdf['step_occupied']

        cdf['safe_step_empty'] = cdf['step_empty']\
            .clip(lower=within_tier_space)
        cdf['safe_step'] = cdf['safe_step_empty'] + cdf['step_occupied']

        cdf['spaced_y'] = (cdf['safe_step']).fillna(0).cumsum()

        min_y = cdf['spaced_y'].iloc[0] - (cdf['span'].iloc[0] / 2.)
        max_y = cdf['spaced_y'].iloc[-1] + (cdf['span'].iloc[-1] / 2.)

        cdf['centered_spaced_y'] = cdf['spaced_y'] - ((max_y + min_y) / 2.)

        # Locate each node within its cluster.
        for _, cluster_row in cdf.iterrows():
            cluster = cluster_row['name']
            cluster_center_y = cluster_row['centered_spaced_y']

            ndf = pd.DataFrame()
            ndf['name'] = list(reversed(sorted(
                node_lists_by_cluster[cluster])))
            ndf['y'] = ndf.index * within_cluster_step
            ndf['y'] = ndf['y'] - ndf['y'].mean() + cluster_center_y

            for _, node_row in ndf.iterrows():
                ys_by_node[node_row['name']] = node_row['y']
                xs_by_node[node_row['name']] = x

    # Organize our nodes and their positions.
    positions_by_node = {
        node: (xs_by_node[node], ys_by_node[node])
        for node in child_lists_by_parent.keys()
    }

    # Assemble the networkx graph.
    G = nx.DiGraph()
    G.add_edges_from([
        (parent, child)
        for parent, children in child_lists_by_parent.items()
        for child in children
    ])

    # Draw the graph.
    fig = plt.figure(figsize=figsize)
    ax = plt.gca()

    # We'll draw the edges and labels first, because we need to know the plot's
    # bounding box to draw the nodes.
    nx.draw_networkx_edges(
        G, positions_by_node, arrowstyle='->', arrowsize=20, width=2, alpha=.2)
    nx.draw_networkx_labels(G, positions_by_node, font_size=14)

    # Examine the plot's bounding box to figure out the scaling factor between
    # our abstract units and matplotlib's points.
    bbox = ax.get_window_extent().transformed(fig.dpi_scale_trans.inverted())
    bbox_width_in_points = bbox.width
    bbox_width_in_abstract_units = ax.get_xlim()[1] - ax.get_xlim()[0]
    bbox_height_in_points = bbox.height
    bbox_height_in_abstract_units = ax.get_ylim()[1] - ax.get_ylim()[0]

    min_points_per_abstract_unit = min(
        bbox_width_in_points / bbox_width_in_abstract_units,
        bbox_height_in_points / bbox_height_in_abstract_units,
    )
    node_size_points = node_size * min_points_per_abstract_unit

    node_size_points = 72 * node_size_points
    node_area_sq_points = node_size_points ** 2

    # Finally, we can plot the nodes.
    nx.draw_networkx_nodes(
        G, positions_by_node, node_size=node_area_sq_points,
        node_color='blue', alpha=.3, node_shape='o', linewidths=2)

    ax.set_axis_off()

    return fig
