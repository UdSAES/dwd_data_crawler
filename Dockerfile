FROM node:9.6.1-alpine

MAINTAINER Florian Wagner

RUN apk add --no-cache make gcc g++ python

RUN mkdir /mnt/downloads && chown node:node /mnt/downloads

ENV DOWNLOAD_DIRECTORY_BASE_PATH=/mnt/downloads

USER node

RUN mkdir /home/node/app

WORKDIR /home/node/app

COPY ./package.json /home/node/app

RUN npm install

COPY ./ /home/node/app/

ENTRYPOINT node index.js
