import os
import sys

import sphinx_rtd_theme

sys.path.insert(0,(os.path.abspath(os.path.join('..','snowshu'))))
sys.path.insert(0,(os.path.abspath('..')))

master_doc = 'index'


extensions = [
    "sphinx_rtd_theme",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
#    "sphinx_autodoc_typehints",
]

html_theme = "sphinx_rtd_theme"
project= "SnowShu"
