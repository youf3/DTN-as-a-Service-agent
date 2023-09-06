FROM icair/dtnaas:agent-master
# Use the dtnaas_dmc2021 image as the base as it has NVMEoF support built in

RUN apt-get -y install nvme-cli ncat

WORKDIR /agent
COPY agent /agent
RUN pip3 install -r requirements.txt

EXPOSE 5000

ENTRYPOINT ["gunicorn", "--bind", "0.0.0.0:5000", "--log-level", "info", "app:app"]
