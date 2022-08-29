FROM docker

ENV SLEEP_TIME='5m'
ENV FILTER_SERVICES=''
ENV TZ='US/Eastern'
ENV VERBOSE='true'

RUN apk add --update --no-cache bash python3 py3-pip curl tzdata
RUN pip3 install docker

COPY ./swarmupd.py /usr/local/bin/swarmupd.py

ENTRYPOINT ["python3", "/usr/local/bin/swarmupd.py"]
