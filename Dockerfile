FROM python:3.9-slim
WORKDIR /app
RUN pip install confluent-kafka numpy
COPY pulse/__init__.py .
COPY pulse ./pulse
ENTRYPOINT ["python", "pulse/darooghe_pulse.py"]
