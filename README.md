This repository contains code for assignments, labs, and demos that are part of MGMT 590 Big Data.
Within the subdirectories are a mix of shell and python scripts, as well as some data files and SQL queries.
You will need to use an appropriate IDE or service to be able to make use of the scripts and files to do something meaningful.

To be able to use the Python code within the subdirs, I suggest that you set up a Python virtual environment and install the python dependencies within requirements.txt in the outermost directory.

In the twitterWithOpenSky_assignement3, there is a Jupyter notebook. You can run this from a local machine by installing the requirements in requirements.txt, informing Jupyter of the virtual python environment where you have all the requirements installed, and then running the runJupyter.sh script. For example, for a virtual environment named "bigdata_venv", you would do:

1. virtualenv -p python3 bigdata_venv
1. source bigdata_venv/bin/activate
1. pip install -r requirements.txt
1. ./runJupyter.sh

(Steps 1-3 only have to be done once to install the libraries into your virtual environment.)
