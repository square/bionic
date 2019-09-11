# Bionic

Bionic is a Python framework for building, running, and sharing data science
research workflows.  It uses dependency injection to automatically gather
together your Python functions into a reusable, configurable tool.

Bionic is currently in alpha.  It is suitable for small-to-medium-size research
projects: i.e., projects that fit in memory, have a small number of
collaborators, and are intended to generate insight or some data artifact (like
a model) rather than run in production.  (These constraints will be eliminated
over time.)  The API will continue to evolve; we will attempt to keep it
backwards-compatible as long as possible, but there will probably be at least
one breaking API change in the future.

Check out the [full documentation](https://bionic.readthedocs.io/en/latest/),
or go straight to [Get
Started](https://bionic.readthedocs.io/en/latest/get-started.html).

## Installation

Bionic can be installed from PyPI:

    pip install bionic[standard]

You'll probably want to install [Graphviz](https://www.graphviz.org/) as well.
See the [Installation
docs](https://bionic.readthedocs.io/en/latest/get-started.html#installation)
for more details on installing and configuring Bionic's dependencies.

## Contributing

See the
[Contribution](https://bionic.readthedocs.io/en/latest/contributing.html)
section of our docs.

## License

Copyright 2019 Square, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
