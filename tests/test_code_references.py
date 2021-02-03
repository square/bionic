import dis
import logging
import pytest

from bionic.code_references import (
    get_code_context,
    get_referenced_objects,
    ReferenceProxy,
    SUPPORTED_INSTRUCTIONS,
)
from bionic.flow import FlowBuilder
from bionic.utils.misc import oneline

global_val = 42


def get_references(func):
    context = get_code_context(func)
    return get_referenced_objects(func.__code__, context)


def test_bytecode_instructions():
    """
    This test cycles through all the opcode instructions and tests that we have
    evaluated them for finding code references.
    """

    for opname in dis.opmap.keys():
        assert opname in SUPPORTED_INSTRUCTIONS


def test_empty_references():
    def x():
        pass

    assert get_references(x) == []

    def x():
        return 42

    assert get_references(x) == []

    def x(val="42"):
        return val

    assert get_references(x) == []

    def x():
        import warnings

        return warnings

    with pytest.warns(UserWarning, match=".*imports the 'warnings' module.*"):
        assert get_references(x) == []


def test_global_references():
    def x():
        return global_val

    assert get_references(x) == [42]


def test_free_references():
    free_val = "42"

    def x():
        return free_val

    assert get_references(x) == ["42"]


def test_cell_references():
    def x():
        cell_val = "42"

        def y():
            return cell_val

    assert get_references(x) == ["cell_val"]


def test_function_references():
    def x():
        return "42"

    def y():
        return x()

    assert get_references(y) == [x]

    def y():
        return oneline("Use a function in another module")

    assert get_references(y) == [oneline]

    def y():
        return func_does_not_exist()  # noqa: F821

    assert get_references(y) == [ReferenceProxy("func_does_not_exist")]


def test_class_references():
    class MyClass:
        def __init__(self):
            self.value = "42"

        @property
        def val(self):
            return self.value

        def log_val(self):
            logging.info(self.value)

    my_class = MyClass()

    def x():
        logging.info(my_class.val)
        my_class.log_val()
        return my_class

    assert get_references(x) == [
        logging.info,
        my_class,
        ReferenceProxy("val"),
        my_class,
        ReferenceProxy("log_val"),
        my_class,
    ]

    def x():
        builder = FlowBuilder()
        builder.assign("cls", MyClass)
        return builder

    assert get_references(x) == [FlowBuilder, ReferenceProxy("assign"), MyClass]


def test_method_references():
    class MyClass:
        def __init__(self):
            self.value = "42"

        def log_val(self):
            logging.log(self.value)

    def x():
        my_class = MyClass()
        my_class.log_val()

    # We don't get the method as a reference because class initialization
    # is a function call and that incorrectly sets my_class as None.
    assert get_references(x) == [MyClass, ReferenceProxy("log_val")]

    def y(my_class):
        my_class.log_val()

    assert get_references(y) == [ReferenceProxy("log_val")]


def test_references_with_qualified_names():
    import multiprocessing

    def x():
        """This function tests LOAD_ATTR opcode"""
        p = multiprocessing.managers.public_methods
        return p(multiprocessing.managers.SyncManager)

    assert get_references(x) == [
        multiprocessing.managers.public_methods,
        multiprocessing.managers.public_methods,
        multiprocessing.managers.SyncManager,
    ]

    def x():
        """This function tests LOAD_METHOD opcode"""
        m = multiprocessing.managers.SyncManager
        return m.start()

    assert get_references(x) == [
        multiprocessing.managers.SyncManager,
        multiprocessing.managers.SyncManager,
        ReferenceProxy("start"),
    ]


def test_multiple_references():
    def get_val(dict, key):
        return dict[key]

    def enclosing_x():
        free_var = "free_val"

        def x(func_def_var="func_def_val"):
            local_var = {"local_key": "local_val"}
            local_val = get_val(local_var, "local_key")
            cell_var = "cell_val"
            logging.log(global_val, free_var, local_val, cell_var, func_def_var)
            builder = FlowBuilder("x")
            # more business logic
            builder.build()

            def inner_x():
                inner_var = "inner_val"

                logging.debug(inner_var, cell_var)

        return x

    assert get_references(enclosing_x()) == [
        get_val,
        logging.log,
        global_val,
        "free_val",
        "cell_var",
        FlowBuilder,
        ReferenceProxy("build"),
        "cell_var",
    ]


def test_conditionals():
    def x(my_class):
        if my_class.val == "42":
            logging.info("INFO log")
        else:
            logging.debug("DEBUG log")

    assert get_references(x) == [ReferenceProxy("val"), logging.info, logging.debug]

    import math

    def y(op_type, val):
        if op_type == "ceil":
            func = math.ceil
        else:
            func = math.floor

        return func(val)

    assert get_references(y) == [math.ceil, math.floor, math.floor]


def test_complex_function():
    # Example code of a ml flow. Taken from below article with minor changes:
    # https://medium.com/towards-artificial-intelligence/machine-learning-algorithms-for-beginners-with-python-code-examples-ml-19c6afd60daa

    # Import required libraries:
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    from sklearn import linear_model
    from sklearn.metrics import r2_score

    # Function for predicting future values :
    def get_regression_predictions(input_features, intercept, slope):
        predicted_values = input_features * slope + intercept
        return predicted_values

    def get_regression_predictions_resized(input_features, intercept, slope):
        predictions = get_regression_predictions(input_features, intercept, slope)
        return [p * 0.75 for p in predictions]

    def predict(resize):
        # Read the CSV file :
        data = pd.read_csv("Fuel.csv")
        data.head()
        # Let"s select some features to explore more :
        data = data[["ENGINESIZE", "CO2EMISSIONS"]]
        # ENGINESIZE vs CO2EMISSIONS:
        plt.scatter(data["ENGINESIZE"], data["CO2EMISSIONS"], color="blue")
        plt.xlabel("ENGINESIZE")
        plt.ylabel("CO2EMISSIONS")
        plt.show()
        # Generating training and testing data from our data:
        # We are using 80% data for training.
        train = data[: (int((len(data) * 0.8)))]
        test = data[(int((len(data) * 0.8))) :]
        # Modeling:
        # Using sklearn package to model data :
        regr = linear_model.LinearRegression()
        train_x = np.array(train[["ENGINESIZE"]])
        train_y = np.array(train[["CO2EMISSIONS"]])
        regr.fit(train_x, train_y)
        # The coefficients:
        logging.info("coefficients : ", regr.coef_)  # Slope
        logging.info("Intercept : ", regr.intercept_)  # Intercept
        # Plotting the regression line:
        plt.scatter(train["ENGINESIZE"], train["CO2EMISSIONS"], color="blue")
        plt.plot(train_x, regr.coef_ * train_x + regr.intercept_, "-r")
        plt.xlabel("Engine size")
        plt.ylabel("Emission")

        # Predicting values:
        # Function for predicting future values :
        if resize:
            prediction_function = get_regression_predictions_resized
        else:
            prediction_function = get_regression_predictions

        # Predicting emission for future car:
        my_engine_size = 3.5
        estimatd_emission = prediction_function(
            my_engine_size, regr.intercept_[0], regr.coef_[0][0]
        )
        logging.info("Estimated Emission :", estimatd_emission)

        # Checking various accuracy:
        test_x = np.array(test[["ENGINESIZE"]])
        test_y = np.array(test[["CO2EMISSIONS"]])
        test_y_ = regr.predict(test_x)
        logging.info(
            "Mean absolute error: %.2f" % np.mean(np.absolute(test_y_ - test_y))
        )
        logging.info(
            "Mean sum of squares (MSE): %.2f" % np.mean((test_y_ - test_y) ** 2)
        )
        logging.info("R2-score: %.2f" % r2_score(test_y_, test_y))

    assert get_references(predict) == [
        pd.read_csv,
        ReferenceProxy("head"),
        plt.scatter,
        plt.xlabel,
        plt.ylabel,
        plt.show,
        ReferenceProxy("int"),
        ReferenceProxy("len"),
        ReferenceProxy("int"),
        ReferenceProxy("len"),
        linear_model.LinearRegression,
        np.array,
        np.array,
        ReferenceProxy("fit"),
        logging.info,
        ReferenceProxy("coef_"),
        logging.info,
        ReferenceProxy("intercept_"),
        plt.scatter,
        plt.plot,
        ReferenceProxy("coef_"),
        ReferenceProxy("intercept_"),
        plt.xlabel,
        plt.ylabel,
        get_regression_predictions_resized,
        get_regression_predictions,
        get_regression_predictions,
        ReferenceProxy("intercept_"),
        ReferenceProxy("coef_"),
        logging.info,
        np.array,
        np.array,
        ReferenceProxy("predict"),
        logging.info,
        np.mean,
        np.absolute,
        logging.info,
        np.mean,
        logging.info,
        r2_score,
    ]
