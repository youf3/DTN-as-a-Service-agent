FROM python:3.6.10-slim AS build

RUN apt update; apt install -y wget make gcc bzip2 libnuma-dev
RUN wget -O nuttcp.tar.bz2 http://nuttcp.net/nuttcp/nuttcp-8.1.4.tar.bz2
RUN tar -xvf nuttcp.tar.bz2; cd nuttcp-8.1.4; make
RUN pip install numa

FROM python:3.6.10-slim
RUN apt update; apt install -y numactl
COPY --from=build /nuttcp-8.1.4/nuttcp-8.1.4 /usr/local/bin/nuttcp
COPY --from=build /root/.cache /root/.cache

COPY agent /agent

RUN pip install -r /agent/requirements.txt

EXPOSE 5000
ENTRYPOINT [ "python"]
CMD ["agent/app.py"]