# arca-fio

## Run the thing

### Requirements

* Current working directory is project root
* Python virtual environment created based on this project's pyproject.toml, and activated
* [fio](https://fio.readthedocs.io/en/latest/fio_doc.html#binary-packages) installed locally


```shell
python fio_plugin.py -f mocks/poisson-rate-submission_input.yaml
```

```shell
python test_fio_plugin.py
```