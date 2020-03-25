FROM python:3.6.10-slim AS build

RUN apt update; apt install -y wget make gcc bzip2
RUN wget -O nuttcp.tar.bz2 http://nuttcp.net/nuttcp/nuttcp-8.1.4.tar.bz2
RUN tar -xvf nuttcp.tar.bz2; cd nuttcp-8.1.4; make

FROM python:3.6.10-slim
COPY --from=build /nuttcp-8.1.4/nuttcp-8.1.4 /usr/local/bin/nuttcp

COPY agent /agent

RUN pip install -r /agent/requirements.txt

ENTRYPOINT [ "python"]
CMD ["agent/app.py"]