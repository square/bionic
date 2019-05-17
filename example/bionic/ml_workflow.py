'''
A toy ML workflow intended to test basic Bionic features.
'''


from sklearn import datasets, model_selection, linear_model, metrics
import pandas as pd

import bionic as bn

bn.util.init_basic_logging()

builder = bn.FlowBuilder()

builder.assign('random_seed', 0)
builder.assign('test_frac', 0.3)


@builder
@bn.persist
def raw_df():
    dataset = datasets.load_breast_cancer()
    df = pd.DataFrame(
        data=dataset.data,
    )
    df['target'] = dataset.target
    return df


# TODO Once we have a decorator that lets us generate multiple targets with
# one function, we should combine this with test_df.
@builder
@bn.persist
def train_df(raw_df, test_frac, random_seed):
    return model_selection.train_test_split(
        raw_df,
        test_size=test_frac,
        random_state=random_seed,
    )[0]


@builder
@bn.persist
def test_df(raw_df, test_frac, random_seed):
    return model_selection.train_test_split(
        raw_df,
        test_size=test_frac,
        random_state=random_seed,
    )[1]


@builder
@bn.persist
def model(train_df):
    m = linear_model.LogisticRegression()
    m.fit(train_df.drop('target', axis=1), train_df['target'])
    return m


@builder
@bn.persist
def pr_df(model, test_df):
    predictions = model.predict_proba(test_df.drop('target', axis=1))[:, 1]
    precisions, recalls, thresholds = metrics.precision_recall_curve(
        test_df['target'], predictions)

    df = pd.DataFrame()
    df['threshold'] = [0] + list(thresholds) + [1]
    df['precision'] = list(precisions) + [1]
    df['recall'] = list(recalls) + [0]

    return df


flow = builder.build()

print flow.get('pr_df')
