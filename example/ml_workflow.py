'''
A toy ML workflow intended to demonstrate basic Bionic features.  Trains a
logistic regression model on the UCI ML Breast Cancer Wisconsin (Diagnostic)
dataset.
'''

import re

from sklearn import datasets, model_selection, linear_model, metrics
import pandas as pd

import bionic as bn

# Initialize our builder.
builder = bn.FlowBuilder('ml_workflow')

# Define some basic parameters.
builder.assign('random_seed', 0)
builder.assign('test_split_fraction', 0.3)
builder.assign('hyperparams_dict', {'C': 1})
builder.assign('feature_inclusion_regex', '.*')


# Load the raw data.
@builder
def raw_frame():
    dataset = datasets.load_breast_cancer()
    df = pd.DataFrame(
        data=dataset.data,
        columns=dataset.feature_names,
    )
    df['target'] = dataset.target
    return df


# Select a subset of the columns to use as features.
@builder
def features_frame(raw_frame, feature_inclusion_regex):
    included_feature_cols = [
        col for col in raw_frame.columns.drop('target')
        if re.match(feature_inclusion_regex, col)
    ]
    return raw_frame[included_feature_cols + ['target']]


# Split the data into train and test sets.
@builder
# The `@outputs` decorator tells Bionic to define two new entities from this
# function (which returns a tuple of two values).
@bn.outputs('train_frame', 'test_frame')
def split_raw_frame(features_frame, test_split_fraction, random_seed):
    return model_selection.train_test_split(
        features_frame,
        test_size=test_split_fraction,
        random_state=random_seed,
    )


# Fit a logistic regression model on the training data.
@builder
def model(train_frame, random_seed, hyperparams_dict):
    m = linear_model.LogisticRegression(
        solver='liblinear', random_state=random_seed,
        **hyperparams_dict)
    m.fit(train_frame.drop('target', axis=1), train_frame['target'])
    return m


# Evaluate the model's precision and recall over a range of threshold values.
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


# Plot the precision against the recall.
@builder
# The `@pyplot` decorator makes the Matplotlib plotting context available to
# our function, then translates our plot into an image object.
@bn.pyplot('plt')
# The `@gather` decorator collects the values of of "hyperparams_dict" and
# "precision_recall_frame" into a single dataframe named "gathered_frame".
# This might not seem very interesting since "gathered_frame" only has one row,
# but it will become useful once we introduce multiplicity.
@bn.gather(
    over='hyperparams_dict',
    also='precision_recall_frame',
    into='gathered_frame')
def all_hyperparams_pr_plot(gathered_frame, plt):
    _, ax = plt.subplots(figsize=(4, 3))
    for row in gathered_frame.itertuples():
        label = ', '.join(
            f'{key}={value}'
            for key, value in row.hyperparams_dict.items()
        )
        row.precision_recall_frame.plot(
            x='recall', y='precision', label=label, ax=ax)
    ax.set_xlabel('Recall')
    ax.set_ylabel('Precision')


# Assemble our flow object.
flow = builder.build()

# Compute and print the precision-recall dataframe.
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
