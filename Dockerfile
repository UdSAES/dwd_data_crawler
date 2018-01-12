FROM node:9.3.0-alpine
MAINTAINER Florian Wagner
RUN mkdir /mnt/downloads && chown node:node /mnt/downloads
VOLUME /mnt/downloads
USER node
RUN mkdir /home/node/app
COPY ./ /home/node/app/
WORKDIR /home/node/app
RUN npm install
ENV DOWNLOAD_DIRECTORY_BASE_PATH=/mnt/downloads
ENTRYPOINT node index.js
