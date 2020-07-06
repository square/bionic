"""
A CLI for running an extended version of the ML example in `ml_workflow`.

Fits and evaluates a model on a scikit-learn dataset.
"""

import pandas as pd
from sklearn import datasets, metrics
import time

import bionic as bn
from .ml_workflow import flow as base_ml_flow

# Add an AUC score summary to our flow.
builder = base_ml_flow.to_builder()


@builder
def auc_score(test_frame, prediction_frame):
    """
    The Area Under the (Receiver Operating Characteristic) Curve.
    """
    return metrics.roc_auc_score(test_frame["target"], prediction_frame["proba"],)


@builder
@bn.gather(over="hyperparams_dict", also="auc_score", into="gathered_frame")
@bn.outputs("best_hyperparams_dict", "best_auc_score")
@bn.docs(
    """The hyperparameter settings with the highest AUC score.""",
    """The best (highest) AUC score, compared over all hyperparameter settings.""",
)
def best_auc_score(gathered_frame):
    best_row = gathered_frame.sort_values("auc_score", ascending=False).iloc[0]
    return best_row[["hyperparams_dict", "auc_score"]]


flow = builder.build()

# Compute and print the model performance.
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Runs a simple ML workflow example")
    parser.add_argument("--bucket", "-b", help="Name of GCS bucket to cache in")
    parser.add_argument(
        "--quiet", "-q", help="Don't enable INFO-level logging", action="store_true"
    )
    parser.add_argument(
        "--parallel", "-p", help="Run flow in parallel", action="store_true"
    )
    parser.add_argument(
        "-C",
        help="Value or values (comma-separated) for"
        "the inverse regularization parameter 'C'",
    )
    parser.add_argument(
        "--big-dataset", "-B", help="Use bigger covertype dataset", action="store_true"
    )

    args = parser.parse_args()
    if not args.quiet:
        bn.util.init_basic_logging()
    if args.bucket is not None:
        flow = flow.setting(
            "core__persistent_cache__gcs__bucket_name", args.bucket
        ).setting("core__persistent_cache__gcs__enabled", True)
    if args.C is not None:
        c_values_str = args.C
        c_values = [
            float(c_value_str.strip()) for c_value_str in c_values_str.split(",")
        ]
        flow = flow.setting("hyperparams_dict", values=[{"C": c} for c in c_values])
    if args.big_dataset:
        builder = flow.to_builder()

        @builder
        @bn.version("covtype dataset")
        def raw_frame():
            dataset = datasets.fetch_covtype()
            feature_names = [f"feature_{ix}" for ix in range(dataset.data.shape[1])]
            df = pd.DataFrame(data=dataset.data, columns=feature_names)
            # This is a multiclass dataset, but we want to treat it as a binary one.
            # We'll just try to detect class 2, since that one is the most common.
            df["target"] = dataset.target == 2
            return df

        flow = builder.build()
    if args.parallel:
        flow = flow.setting("core__parallel_execution__enabled", True)

    start = time.time()
    all_hpds = flow.get("hyperparams_dict", collection=list)
    best_hpd = flow.get("best_hyperparams_dict")
    best_auc_score = flow.get("best_auc_score")  # noqa: F811
    end = time.time()

    print(f"Number of hyperparameter configurations tested: {len(all_hpds)}")
    print(f"Best hyperparameter configuration: {best_hpd!r}")
    print(f"Best AUC: {best_auc_score!r}")
    print(f"Total time taken: {end - start}")
