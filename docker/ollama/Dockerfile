FROM ollama/ollama:latest

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

RUN ollama serve & \
    until curl -s http://localhost:11434/api/tags > /dev/null; do sleep 1; done && \
    ollama pull bge-small-en

EXPOSE 11434
