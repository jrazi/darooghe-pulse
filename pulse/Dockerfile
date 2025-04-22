FROM python:3.9-slim
WORKDIR /pulse
COPY darooghe_pulse.py .
RUN pip install confluent-kafka
ENTRYPOINT ["python", "darooghe_pulse.py"]
