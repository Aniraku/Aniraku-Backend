FROM golang:alpine AS gobuild
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -tags web -ldflags="-s -w" -o /aniraku-server ./cmd/aniraku-server

FROM node:22-alpine
RUN apk add --no-cache ca-certificates curl
WORKDIR /app
COPY --from=gobuild /aniraku-server /app/aniraku-server
COPY start.sh /start.sh
RUN chmod +x /start.sh
EXPOSE 43211
CMD ["/start.sh"]
