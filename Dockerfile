FROM python:3.9-slim
COPY pulse ./pulse
RUN pip install confluent-kafka
ENTRYPOINT ["python", "pulse/darooghe_pulse.py"]
