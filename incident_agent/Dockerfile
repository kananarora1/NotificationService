FROM python:3.12-alpine3.24@sha256:e064671ca452259bab50e8e2c7bd896e39e9a343bb06ca25234adf6fe18d0fc7

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY incident_agent/agent.py incident_agent/investigation.py incident_agent/__init__.py /app/incident_agent/

USER 10001:10001
EXPOSE 8080
CMD ["python", "-m", "incident_agent.agent"]
