FROM quay.io/centos/centos:stream8

RUN dnf -y module install python39 && dnf -y install python39 python39-pip git make gcc fio
RUN mkdir /plugin
COPY requirements.txt /plugin
RUN pip3 install --requirement /plugin/requirements.txt
COPY fio_plugin.py /plugin
COPY fio_schema.py /plugin

WORKDIR /plugin
# USER 1000
ENTRYPOINT ["python3.9", "fio_plugin.py", "-f", "/plugin/fio-job.yaml", "--debug" ]
CMD []

LABEL org.opencontainers.image.source="https://github.com/mfleader/arca-fio/blob/main/fio_plugin.py"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL org.opencontainers.image.vendor="Arcalot project"
LABEL org.opencontainers.image.authors="Arcalot contributors"
LABEL org.opencontainers.image.title="Fio Arcalot Plugin"
LABEL io.github.arcalot.arcaflow.plugin.version="1"