FROM python:3.10-slim-bullseye

ENV SSE_PORT='50050'
ENV SSE_ALLOWSCRIPT='false'

WORKDIR /sse

COPY /src ./src

SHELL ["/bin/bash", "-c"]

RUN python -m pip install --upgrade pip

RUN python -m pip install virtualenv && \
    virtualenv venv && \
    source venv/bin/activate

RUN python -m pip install --no-cache-dir -r src/requirements.txt

EXPOSE ${SSE_PORT}

CMD ["python","src"]