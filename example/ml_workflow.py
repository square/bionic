'''
A toy ML workflow intended to demonstrate basic Bionic features.  Trains a
logistic regression model on the UCI ML Breast Cancer Wisconsin (Diagnostic)
dataset.
'''

import re

from sklearn import datasets, model_selection, linear_model, metrics
import pandas as pd

import bionic as bn

builder = bn.FlowBuilder('ml_workflow')

builder.assign('random_seed', 0)
builder.assign('test_split_fraction', 0.3)
builder.assign('hyperparams_dict', {'C': 1})
builder.assign('feature_inclusion_regex', '.*')


@builder
def raw_frame():
    dataset = datasets.load_breast_cancer()
    df = pd.DataFrame(
        data=dataset.data,
        columns=dataset.feature_names,
    )
    df['target'] = dataset.target
    return df


@builder
def features_frame(raw_frame, feature_inclusion_regex):
    included_feature_cols = [
        col for col in raw_frame.columns.drop('target')
        if re.match(feature_inclusion_regex, col)
    ]
    return raw_frame[included_feature_cols + ['target']]


@builder
@bn.outputs('train_frame', 'test_frame')
def split_raw_frame(features_frame, test_split_fraction, random_seed):
    return model_selection.train_test_split(
        features_frame,
        test_size=test_split_fraction,
        random_state=random_seed,
    )


@builder
def model(train_frame, random_seed, hyperparams_dict):
    m = linear_model.LogisticRegression(
        solver='liblinear', random_state=random_seed,
        **hyperparams_dict)
    m.fit(train_frame.drop('target', axis=1), train_frame['target'])
    return m


@builder
def precision_recall_frame(model, test_frame):
    predictions = model.predict_proba(test_frame.drop('target', axis=1))[:, 1]
    precisions, recalls, thresholds = metrics.precision_recall_curve(
        test_frame['target'], predictions)

    df = pd.DataFrame()
    df['threshold'] = [0] + list(thresholds) + [1]
    df['precision'] = list(precisions) + [1]
    df['recall'] = list(recalls) + [0]

    return df


@builder
@bn.pyplot('plt')
@bn.gather(
    over='hyperparams_dict',
    also='precision_recall_frame',
    into='gathered_frame')
def all_hyperparams_pr_plot(gathered_frame, plt):
    ax = plt.subplot()
    for row in gathered_frame.itertuples():
        label = ', '.join(
            '%s=%s' % key_value
            for key_value in row.hyperparams_dict.items()
        )
        row.precision_recall_frame.plot(
            x='recall', y='precision', label=label, ax=ax)


flow = builder.build()

if __name__ == '__main__':
    bn.util.init_basic_logging()

    import argparse
    parser = argparse.ArgumentParser(
        description='Runs a simple ML workflow example')
    parser.add_argument(
        '--bucket', '-b', help='Name of GCS bucket to cache in')

    args = parser.parse_args()
    if args.bucket is not None:
        flow = flow\
            .setting('core__persistent_cache__gcs__bucket_name', args.bucket)\
            .setting('core__persistent_cache__gcs__enabled', True)

    with pd.option_context("display.max_rows", 10):
        print(flow.get('precision_recall_frame'))
