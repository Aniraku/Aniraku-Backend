FROM golang:alpine AS gobuild
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /aniraku-server ./cmd/aniraku-server

FROM python:3.12-slim AS pybase
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY Aniraku\ Backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
ENV VIPERTLS_HOME=/app/vipertls
RUN vipertls install-browsers --with-deps && rm -rf /var/lib/apt/lists/*

FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
ENV VIPERTLS_HOME=/app/vipertls
COPY --from=pybase /app/vipertls /app/vipertls
COPY --from=pybase /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=gobuild /aniraku-server /app/aniraku-server
COPY Aniraku\ Backend/cmd/miruro-proxy/proxy.py /app/proxy.py
COPY Aniraku\ Backend/start.sh /start.sh
RUN chmod +x /start.sh
EXPOSE 43211
CMD ["/start.sh"]
