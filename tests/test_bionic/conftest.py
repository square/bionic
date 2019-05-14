import sys
import os
# This allows us to import helper functions from any subdirectory of our tests.
# It's based on this answer: https://stackoverflow.com/questions/33508060/
# The more straightforward approach of creating packages in the test/ dirs
# seems to be discouraged in pytest.
sys.path.append(os.path.join(os.path.dirname(__file__), '_helpers'))
