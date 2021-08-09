FROM fedora:33
MAINTAINER "Jan Hutar" <jhutar@redhat.com>

RUN dnf -y install dumb-init git-core python3-setuptools python3-pip gcc 'dnf-command(builddep)' \
    && dnf -y builddep python3-psutil \
    && rm -rf /var/cache/yum/* /var/cache/dnf/*
RUN echo "Marker 2021-08-09 10:0" \
    && pip install -U pip \
    && pip install -U git+https://github.com/redhat-performance/opl.git \
    && pip install -U git+https://github.com/jhutar/kafka-e2e-perf-test.git

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["sleep", "infinity"]
