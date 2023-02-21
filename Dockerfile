FROM alpine

LABEL MAINTAINER="max@maxtpower.com"

RUN apk add bash curl jq --no-cache
COPY . /opt/tools
WORKDIR /opt/tools
