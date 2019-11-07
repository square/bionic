import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from scipy.stats import multivariate_normal

import bionic as bn

builder = bn.FlowBuilder('intro')

builder.assign('random_seed', 0)
builder.assign('variance', 2)
builder.assign('correlation', 0.5)
builder.assign('n_samples', 1000)


@builder
def my_random_df(random_seed, variance, correlation, n_samples):
    data = multivariate_normal(
            mean=[0, 0],
            cov=[
                [variance, correlation * variance],
                [correlation * variance, variance],
            ],
        ).rvs(
            size=n_samples,
            random_state=random_seed,
        )
    return pd.DataFrame(columns=['x', 'y'], data=data)


@builder
def my_model(my_random_df):
    model = LinearRegression()
    model.fit(my_random_df[['x']], my_random_df['y'])
    return model


@builder
def est_correlation(my_model):
    return my_model.coef_[0]


@builder
def est_intercept(my_model):
    return my_model.intercept_


@builder
@bn.pyplot('plt')
def my_plot(my_random_df, est_correlation, est_intercept, plt):
    with plt.style.context('seaborn-whitegrid'):
        plt.scatter(my_random_df['x'], my_random_df['y'], alpha=0.2)

        line_xs = np.array([
            my_random_df['x'].min(),
            my_random_df['x'].max(),
        ])
        line_ys = (line_xs + est_correlation) + est_intercept
        plt.plot(line_xs, line_ys)


flow = builder.build()

if __name__ == '__main__':
    bn.util.init_basic_logging()

    print("Estimated intercept:", flow.get('est_intercept'))
    print("Estimated correlation:", flow.get('est_correlation'))
