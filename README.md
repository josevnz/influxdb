# Influxdb Datasets

A small project/ tutorial I put together while learning how to use Influxdb for data processing.

This tutorial includes 2 different public data sets to play with, not personal identifiable information on any of them.

Please read the [](tutorial/README.md) file for details.

# Developer mode

```shell
python -m venv ~/virtualenv/influxdb_dataset
. ~/virtualenv/influxdb_dataset/bin/activate
pip install --upgrade pip
pip install --upgrade setuptools
pip install --upgrade build
pip install --upgrade wheel
python setup.py develop
```

And if you want to build a wheel to install somewhere else

```shell
. ~/virtualenv/influxdb_dataset/bin/activate
python -m build --wheel .
```
