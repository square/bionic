'''
A toy ML workflow intended to test basic Bionic features.
'''

from sklearn import datasets, model_selection, linear_model, metrics
import pandas as pd

import bionic as bn

builder = bn.FlowBuilder('ml_workflow')

builder.assign('random_seed', 0)
builder.assign('test_frac', 0.3)
builder.assign('hyperparams_dict', {'C': 1})


@builder
def raw_df():
    dataset = datasets.load_breast_cancer()
    df = pd.DataFrame(
        data=dataset.data,
    )
    df['target'] = dataset.target
    return df


@builder
@bn.outputs('train_df', 'test_df')
def split_raw_df(raw_df, test_frac, random_seed):
    return model_selection.train_test_split(
        raw_df,
        test_size=test_frac,
        random_state=random_seed,
    )


@builder
def model(train_df, random_seed, hyperparams_dict):
    m = linear_model.LogisticRegression(
        solver='liblinear', random_state=random_seed,
        **hyperparams_dict)
    m.fit(train_df.drop('target', axis=1), train_df['target'])
    return m


@builder
def pr_df(model, test_df):
    predictions = model.predict_proba(test_df.drop('target', axis=1))[:, 1]
    precisions, recalls, thresholds = metrics.precision_recall_curve(
        test_df['target'], predictions)

    df = pd.DataFrame()
    df['threshold'] = [0] + list(thresholds) + [1]
    df['precision'] = list(precisions) + [1]
    df['recall'] = list(recalls) + [0]

    return df


@builder
@bn.pyplot('plt')
def pr_plot(pr_df, plt):
    ax = plt.subplot()
    pr_df.plot(x='recall', y='precision', ax=ax)


flow = builder.build()

if __name__ == '__main__':
    from pathlib2 import Path

    bn.util.init_basic_logging()
    bn.util.init_matplotlib()

    dag_path = Path('example_output')
    if not dag_path.exists():
        dag_path.mkdir()
    fig = flow.plot_dag()
    fig.savefig(str(dag_path / 'dag_test.png'))

    flow.get('pr_plot')
    with pd.option_context("display.max_rows", 10):
        print(flow.get('pr_df'))
