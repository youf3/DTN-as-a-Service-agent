FROM icair/dtnaas:agent-master
# Use the icair/dtnaas image as the base as it has NVMEoF support built in

RUN apt-get -y install nvme-cli ncat

WORKDIR /agent
COPY agent /agent
RUN pip3 install -r requirements.txt

# default port
EXPOSE 5000

# runtime environment vars
ENV LOG_LEVEL="info"

ENTRYPOINT ["gunicorn", "-w", "1", "--threads", "16", "--bind", "0.0.0.0:5000", "--access-logfile", "-", "--access-logformat", "%(h)s %(t)s \"%(r)s\" %(s)s %(b)s %(M)s", "app:app"]
