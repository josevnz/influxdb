# InfluxDBIntro

A small project/ tutorial I put together while learning how to use Influxdb for data processing.

# Developer mode

```shell
python -m venv ~/virtualenv/influxdbintro
. ~/virtualenv/influxdbintro/bin/activate
pip install --upgrade pip
pip install --upgrade build
pip install --upgrade wheel
python setup.py develop
```

And if you want to build a wheel to install somewhere else

```shell
. ~/virtualenv/influxdbintro/bin/activate
python -m build --wheel .
```