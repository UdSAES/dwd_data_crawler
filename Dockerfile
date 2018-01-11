FROM node:9.3.0-alpine
MAINTAINER Florian Wagner
USER node
RUN mkdir /home/node/app
COPY ./ /home/node/app/
WORKDIR /home/node/app
VOLUME /mnt/downloads
RUN npm install
ENV DOWNLOAD_DIRECTORY_BASE_PATH=/mnt/downloads
ENTRYPOINT node index.js
