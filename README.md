This repository contains code for assignments, labs, and demos that are part of MGMT 590 Big Data.
Within the subdirectories are a mix of shell and python scripts, as well as some data files and SQL queries.
You will need to use an appropriate IDE or service to be able to make use of the scripts and files to do something meaningful.

To be able to use the Python code within the subdirs, I suggest that you set up a Python virtual environment and install the python dependencies within requirements.txt in the outermost directory. For example:
1. Download the code as a zip file and unzip the file. This will create a folder named edu.purdue.krannert.bigdata-master with all the contents of the zip file expanded.
1. Move the contents of the expanded folder to where you want to install the code, such as a folder named bigdata in Documents or your home directory.
1. Change directory to be within the code directory. (In my case, I am in $HOME/CODE/bigdata.)
1. You should be looking at the directory structure above.

Now, set up a virtual python environment named "bigdata_venv", activate the python environment and install all of the dependencies the code requires.
1. virtualenv -p python3 bigdata_venv
1. source bigdata_venv/bin/activate
1. pip install -r requirements.txt

The requirements.txt file contains a list of all the libraries that the code in this repository depends on. If the last command with "pip install -r" fails, you will need to resolve the issue or else some of the code may not run because libraries will be missing.

In the twitterWithOpenSky_assignement3, there is a Jupyter notebook. You can run the Jupyter notebook and have it recognize the code in the subdirectories (actually, all of those named "python").
To inform Jupyter of your own virtual environment, execute the following (assuming your virtual environment is named "bigdata_venv"):
> python -m ipykernel install --name=bigdata_venv

Then you can run the script that starts Jupyter on your local machine:
> ./runJupyter.sh
