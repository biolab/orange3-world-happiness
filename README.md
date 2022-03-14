Orange3 World Happiness
======================

Orange3 World Happiness is an add-on for the [Orange3](http://orange.biolab.si) data mining suite. It provides widgets 
for accessing socioeconomic data from various databases such as [WHR](https://worldhappiness.report/), 
[WDI](https://data.worldbank.org/), [OECD](https://stats.oecd.org/).

Installation
------------

To install the add-on from source run

    pip install .

To register this add-on with Orange, but keep the code in the development directory (do not copy it to 
Python's site-packages directory), run

    pip install -e .

Documentation / widget help can be built by running

    make html htmlhelp

from the doc directory.

Usage
-----

After the installation, the widget from this add-on is registered with Orange. To run Orange from the terminal,
use

    orange-canvas

or

    python -m Orange.canvas

The new widget appears in the toolbox bar under the World Happiness section.
