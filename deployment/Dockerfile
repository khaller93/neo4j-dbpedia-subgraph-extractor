FROM python:3.9.16-slim-bullseye

RUN adduser --disabled-password --gecos '' ohm
RUN mkdir /var/lib/subsampler \
    && chown ohm:ohm -R /var/lib/subsampler

USER ohm

COPY . /opt/dbpediasampler

RUN pip install -r /opt/dbpediasampler/requirements.txt

VOLUME /var/lib/subsampler
WORKDIR /var/lib/subsampler

ENTRYPOINT ["python", "/opt/dbpediasampler/main.py"]