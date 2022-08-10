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

## Terms

(rusage documentation)[https://docs.oracle.com/cd/E36784_01/html/E36870/rusage-1b.html]

* `nvcsw` Number of voluntary context switches
* `nivcsw` Number of involuntary context switches
* `minflt` page faults requiring physical IO
* `majflt` page faults not requiring physical IO

