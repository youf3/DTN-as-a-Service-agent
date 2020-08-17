FROM python:3.6.10-slim AS build

RUN apt update; apt install -y wget make gcc bzip2 libnuma-dev git
RUN wget -O nuttcp.tar.bz2 http://nuttcp.net/nuttcp/nuttcp-8.1.4.tar.bz2
RUN tar -xvf nuttcp.tar.bz2; cd nuttcp-8.1.4; make
RUN git clone https://github.com/jbd/msrsync.git; cd msrsync; chmod +x msrsync
RUN pip install --user numa psutil

FROM python:3.6.10-slim
RUN apt update; apt install -y curl
RUN curl -s https://packagecloud.io/install/repositories/akopytov/sysbench/script.deb.sh | bash
RUN apt install -y numactl fio python rsync sysbench
COPY --from=build /nuttcp-8.1.4/nuttcp-8.1.4 /usr/local/bin/nuttcp
COPY --from=build /msrsync/msrsync /usr/local/bin/msrsync
COPY --from=build /root/.cache /root/.cache
COPY --from=build /root/.local /root/.local

COPY agent /agent

RUN pip install -r /agent/requirements.txt

EXPOSE 5000
ENTRYPOINT [ "python"]
CMD ["agent/app.py"]