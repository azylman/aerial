FROM ollama/ollama:latest

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

RUN ollama serve & OLLAMA_PID=$! && \
    until curl -s http://localhost:11434/api/tags > /dev/null; do sleep 1; done && \
    ollama pull bge-small-en && \
    kill $OLLAMA_PID && \
    wait $OLLAMA_PID 2>/dev/null || true

EXPOSE 11434
