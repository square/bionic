# An example docker image that can be used to test the AIP integration
FROM python:3.8

WORKDIR /code

COPY README.md setup.py /code/
COPY bionic/deps/ /code/bionic/deps
RUN ls /code/*


RUN python setup.py egg_info && \
    sed '/^\[/d' bionic.egg-info/requires.txt | sort | uniq >> requirements.txt && \
    pip install -r requirements.txt

COPY . ./

RUN pip install -e .
