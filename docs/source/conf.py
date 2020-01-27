import sphinx_rtd_theme

import sys, os
sys.path.insert(0,(os.path.abspath(os.path.join('..','..','snowshu'))))



extensions = [
    "sphinx_rtd_theme",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_autodoc_typehints",
]

html_theme = "sphinx_rtd_theme"
project= "SnowShu"
