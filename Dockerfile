FROM node:9.6.1-alpine

MAINTAINER Florian Wagner

RUN apk add --no-cache make gcc g++ python bzip2 lz4

RUN mkdir /mnt/downloads && chown node:node /mnt/downloads

ENV DOWNLOAD_DIRECTORY_BASE_PATH=/mnt/downloads

USER node

RUN mkdir /home/node/app
WORKDIR /home/node/app

COPY --chown=node:node ./package.json  ./package-lock.json /home/node/app/

RUN npm install --production

COPY --chown=node:node ./configuration/ /home/node/app/
COPY --chown=node:node ./scripts/ /home/node/app/
COPY --chown=node:node ./lib /home/node/app/
COPY --chown=node:node ./index.js /home/node/app/

ENTRYPOINT node index.js
