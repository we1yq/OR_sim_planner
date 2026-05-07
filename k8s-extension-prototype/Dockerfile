FROM python:3.13-slim

WORKDIR /app
ENV HOME=/tmp

COPY controller/requirements.txt /app/controller/requirements.txt
RUN pip install --no-cache-dir -r /app/controller/requirements.txt

COPY controller /app/controller
COPY manifests /app/manifests
COPY mock /app/mock
COPY simulation_core /app/simulation_core

USER 65532:65532

ENTRYPOINT ["python3", "/app/controller/main.py"]
CMD ["--run-controller", "--namespace", "or-sim"]
