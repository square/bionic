import pickle

from bionic.tokenization import tokenize


def test_tokenize_straight_translation():
    assert tokenize(1) == '1'
    assert tokenize(1.0) == '1.0'
    assert tokenize('hello') == 'hello'


def test_tokenize_simple_cleaning():
    assert tokenize('Hello').startswith('hello_')
    assert tokenize(True).startswith('true_')
    assert tokenize('test\x00').startswith('test._')


def test_avoid_initial_period():
    assert tokenize('.test').startswith('_.test')
    assert tokenize('\x00\x00').startswith('_..')


def test_ensure_token_length_is_capped():
    assert len(tokenize('a' * 1000)) < 50


class Point(object):
    def __init__(self, x, y):
        self.x = x
        self.y = y


def test_tokenize_complex_type():
    token = tokenize(Point(1, 2), pickle.dumps)
    assert isinstance(token, str)
    assert len(token) == 10


def test_tokenize_no_collisions():
    points = [
        Point(x, y)
        for x in range(100)
        for y in range(100)
    ]
    tokens = [
        tokenize(point, pickle.dumps)
        for point in points
    ]
    assert len(set(tokens)) == len(points)
