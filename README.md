This repository contains code for assignments, labs, and demos that are part of MGMT 590 Big Data.
Within the subdirectories are a mix of shell and python scripts, as well as some data files and SQL queries.
You will need to use an appropriate IDE or service to be able to make use of the scripts and files to do something meaningful.

To be able to use the Python code within the subdirs, I suggest that you set up a Python virtual environment and install the python dependencies within requirements.txt in the outermost directory. For example, to set up a virtual environment named "bigdata_venv", do the following:
1. virtualenv -p python3 bigdata_venv
1. source bigdata_venv/bin/activate
1. pip install -r requirements.txt

In the twitterWithOpenSky_assignement3, there is a Jupyter notebook. You can run the Jupyter notebook and have it recognize the code in the subdirectories (actually, all of those named "python").
To inform Jupyter of your own virtual environment, execute the following (assuming your virtual environment is named "bigdata_venv"):
> python -m ipykernel install --name=bigdata_venv

Then you can run the script that starts Jupyter on your local machine:
> ./runJupyter.sh
