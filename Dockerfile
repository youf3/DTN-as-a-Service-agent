FROM icair/dtnaas_dmc2021:latest
# Use the dtnaas_dmc2021 image as the base as it has NVMEoF support built in

WORKDIR /agent
COPY agent /agent
RUN pip3 install -r requirements.txt

EXPOSE 5000
ENTRYPOINT [ "python3"]
CMD ["app.py"]
