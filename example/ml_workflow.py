"""
A toy ML workflow intended to demonstrate basic Bionic features.  Trains a
logistic regression model on the UCI ML Breast Cancer Wisconsin (Diagnostic)
dataset.
"""

import re

import pandas as pd
from sklearn import datasets, linear_model, metrics, model_selection

import bionic as bn

# Initialize our builder.
builder = bn.FlowBuilder("ml_workflow")

# Define some basic parameters.
builder.assign(
    "random_seed", 0, doc="Arbitrary seed for all random decisions in the flow."
)
builder.assign(
    "test_split_fraction", 0.3, doc="Fraction of data to include in test set."
)
builder.assign(
    "hyperparams_dict", {"C": 1}, doc="Hyperparameters to use when training the model."
)
builder.assign(
    "feature_inclusion_regex",
    ".*",
    doc="Regular expression specifying which feature names to include.",
)


# Load the raw data.
@builder
def raw_frame():
    """
    The raw data, including all features and a `target` column of labels.
    """
    dataset = datasets.load_breast_cancer()
    df = pd.DataFrame(data=dataset.data, columns=dataset.feature_names)
    df["target"] = dataset.target
    return df


# Select a subset of the columns to use as features.
@builder
def features_frame(raw_frame, feature_inclusion_regex):
    """Labeled data with a selected subset of the feature columns."""
    included_feature_cols = [
        col
        for col in raw_frame.columns.drop("target")
        if re.match(feature_inclusion_regex, col)
    ]
    return raw_frame[included_feature_cols + ["target"]]


# Split the data into train and test sets.
@builder
# The `@outputs` decorator tells Bionic to define two new entities from this
# function (which returns a tuple of two values).
@bn.outputs("train_frame", "test_frame")
@bn.docs(
    "Subset of feature data rows, used for model training.",
    "Subset of feature data rows, used for model testing.",
)
def split_raw_frame(features_frame, test_split_fraction, random_seed):
    return model_selection.train_test_split(
        features_frame, test_size=test_split_fraction, random_state=random_seed,
    )


# Fit a logistic regression model on the training data.
@builder
def model(train_frame, random_seed, hyperparams_dict):
    """A binary classifier sklearn model."""
    m = linear_model.LogisticRegression(
        solver="liblinear", random_state=random_seed, **hyperparams_dict
    )
    m.fit(train_frame.drop("target", axis=1), train_frame["target"])
    return m


# Predict probabilities for the test data.
@builder
def prediction_frame(model, test_frame):
    """
    A dataframe with one column, `proba`, containing predicted probabilities for the
    test data.
    """
    predictions = model.predict_proba(test_frame.drop("target", axis=1))[:, 1]
    df = pd.DataFrame()
    df["proba"] = predictions
    return df


# Evaluate the model's precision and recall over a range of threshold values.
@builder
def precision_recall_frame(test_frame, prediction_frame):
    """
    A dataframe with three columns:
    - `threshold`: a probability threshold for the model
    - `precision`: the test set precision resulting from that threshold
    - `recall`: the test set recall resulting from that threshold
    """
    precisions, recalls, thresholds = metrics.precision_recall_curve(
        test_frame["target"], prediction_frame["proba"],
    )

    df = pd.DataFrame()
    df["threshold"] = [0] + list(thresholds) + [1]
    df["precision"] = list(precisions) + [1]
    df["recall"] = list(recalls) + [0]

    return df


# Plot the precision against the recall.
@builder
# The `@pyplot` decorator makes the Matplotlib plotting context available to
# our function, then translates our plot into an image object.
@bn.pyplot("plt")
# The `@gather` decorator collects the values of of "hyperparams_dict" and
# "precision_recall_frame" into a single dataframe named "gathered_frame".
# This might not seem very interesting since "gathered_frame" only has one row,
# but it will become useful once we introduce multiplicity.
@bn.gather(
    over="hyperparams_dict", also="precision_recall_frame", into="gathered_frame"
)
def all_hyperparams_pr_plot(gathered_frame, plt):
    """
    A plot of precision against recall.  Includes one curve for each set of
    hyperparameters.
    """
    _, ax = plt.subplots(figsize=(4, 3))
    for row in gathered_frame.itertuples():
        label = ", ".join(
            f"{key}={value}" for key, value in row.hyperparams_dict.items()
        )
        row.precision_recall_frame.plot(x="recall", y="precision", label=label, ax=ax)
    ax.set_xlabel("Recall")
    ax.set_ylabel("Precision")


# Assemble our flow object.
flow = builder.build()
