# Documentation for CliDEsc's Python Common Utilities

## Description

This folder contains the source files used to generate Python Sphinx technical web documentation for CliDEsc's Common Utilities. In addition to Sphinx a Read the Docs theme is used and the web documentation is hosted using gitlab pages.

Information about Sphinx can be found here [Sphinx](https://www.sphinx-doc.org/en/master/).

Information about Read the Docs can be found here [Read the Docs](https://readthedocs.org/)

There are many online tutorials describing how to get started with Sphinx documentation and a selection of these are listed below:

* [An idiot’s guide to Python documentation with Sphinx and ReadTheDocs](https://samnicholls.net/2016/06/15/how-to-sphinx-readthedocs/)
* [Documenting Python Packages with Sphinx and ReadTheDocs](https://brendanhasz.github.io/2019/01/05/sphinx.html)
* [Pycharm Sphinx Tutorial](https://www.jetbrains.com/pycharm/guide/tutorials/sphinx_sites/)
* [Documenting Python Code](https://realpython.com/documenting-python-code/)

The URL to the hosted documentation is [CliDEsc Python helper function reference](https://clidesc-niwa.gitlab.io/clidesc-common/index.html)

## Installation

The steps for installing Sphinx, adding any additional extentions and initialising a new `docs/` folder

Install Sphinx:

```
pip install Sphinx

```

Initialise the `docs/` directory with `sphinx-quickstart`:

```
mkdir docs
cd docs/
sphinx-quickstart

```

Follow the prompts and enable `autodoc`, `intersphinx` and `viewcode`.

## Automatically generate module API docs

Using the `autodoc` functionality and the `sphinx-apidoc` command Sphinx can automatically build nice web documentation from the `docstrings` of the module, functions and classes. 

```
cd docs/
sphinx-apidoc -o outputdir ../<package>

```

where `outputdir` should be set to `source/` and `<package>` should point to your python code. In this case where the `clide.py` file lives.

## Configuration

`Sphinx` is configured by a `conf.py` file located in the `docs/` folder. Using `sphinx-quickstart` most of the options have been setup, but there are a few things to add or alter. Edit the `conf.py` file as described below.

### Module paths

The `clide.py` Python module sits in a different directory and in order for Sphix to find this file the directory has to be added to `sys.path`. In the `conf.py` file `# -- Path setup` section add this code...

``` python
# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
sys.path.insert(0, os.path.abspath('../../clidesc/'))

```

### Project Information

This is where the project name, copyright, author name and version number information goes...

``` python
# -- Project information -----------------------------------------------------

project = 'CliDEsc helper functions'
copyright = '2021, James Sturman'
author = 'James Sturman'

# The full version, including alpha/beta/rc tags
release = '0.1'

```

### Extentions

In the next `conf.py` section a list of extensions is provided. Most of these are installed during the Sphinx installation however there are a large number of Sphinx extensions provided by the user community. These can be explored here [Sphinx contribution GitHub](https://github.com/sphinx-contrib). Any additional extensions will need to be installed using `pip` prior to adding them to the extentions list.

``` python
# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx_rst_builder',
    'sphinx.ext.autosummary',
    'sphinx.ext.viewcode',
    ]

```

### Options for HTML output

This section is where you specify the html theme to use, theme options such as a logo file and the location of any custom static files such as style sheets. I've used a custom `my_theme.css` file to adjust how tables are presented and the width of the documentation content pane. This is stored in the `_static\\css` folder. You can also add custom javascript in the same way storing `.js` files in a `_static\\js` folder.

``` python
# -- Options for HTML output -------------------------------------------------

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

# These paths are either relative to html_static_path
# or fully qualified paths (eg. https://)
html_css_files = [
    'css/my_theme.css',
]

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'sphinx_rtd_theme'
html_theme_options = {
    'logo_only': False
}
html_logo = "./_images/NIWA_Rev_Hor_2.png"


```

### Options for autodoc

Options that control the behaviour of autodoc tools can be added too...

```python
# Autodoc settings
autosummary_generate = True
autodoc_member_order = 'bysource'

```

## index.rst

By default the `sphinx-quickstart` command creates the index.rst file and some changes to this may be required. For instance the table of contents tree settings can be adjusted to have a caption name...

```
.. toctree::
   :maxdepth: 2
   :caption: Contents:

   clide.rst


```

## clide.rst

This file contains the autodoc calls to extract the docstring content for the `clide.py` module. The `automodule` directive was initially tested with `autosummary`. This created documentation for all functions within `clide.py` and a summary table. However, each function lacked a section heading and my preference was to use the `autofunction` directive and manually adding section headings. The section headings are added to the table of content menue which I prefer.

For example...

```
clide_open
----------

.. autofunction:: clide.clide_open


```

To continue with this approach any new function that's added to `clide.py` will need to be added to the `clide.rst` file. If the `automodule` directive option was used any new functions would automatically be added to the documentaion.

## Building the HTML files

Buidling the HTML documentation is done using the `make` command from the `docs/` folder...

```
make html
```


