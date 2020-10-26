# You can set these variables from the command line, and also from the environment for
# the first two.
SPHINXOPTS    ?=
SPHINXBUILD   ?= sphinx-build
SOURCEDIR     = .
BUILDDIR      = _build

# Put this first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

open: html
	open _build/html/index.html

livehtml:
	sphinx-autobuild --ignore '*.swp' --ignore 'tutorials/bndata/**/*' --ignore 'tutorials/.ipynb_checkpoints/**/*' -b html $(ALLSPHINXOPTS) . $(BUILDDIR)/html

.PHONY: help Makefile

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
