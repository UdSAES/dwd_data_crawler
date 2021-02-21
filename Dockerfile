# SPDX-FileCopyrightText: 2018 UdS AES <https://www.uni-saarland.de/lehrstuhl/frey.html>
# SPDX-License-Identifier: MIT

# Start at current LTS release, but specify version explicitly
FROM node:14-alpine3.11 AS production

# Provide metadata according to namespace suggested by http://label-schema.org/
LABEL org.label-schema.schema-version="1.0.0-rc.1"
LABEL org.label-schema.name="dwd_data_crawler"
LABEL org.label-schema.description="Software agent for crawling data from opendata.dwd.de"
LABEL org.label-schema.vendor="UdS AES"
LABEL org.label-schema.vcs-url="https://github.com/UdSAES/dwd_data_crawler"

# Install dependencies on the base image level
RUN apk add --no-cache make gcc g++ python bzip2 lz4

# Prepare directories and environment
ENV NODE_ENV=production
ENV WORKDIR=/home/node/app
ENV DOWNLOAD_DIR=/mnt/downloads

RUN mkdir $DOWNLOAD_DIR && chown node:node $DOWNLOAD_DIR

USER node
RUN mkdir $WORKDIR
WORKDIR $WORKDIR

# Configure application according to directory structure created
ENV DOWNLOAD_DIRECTORY_BASE_PATH=$DOWNLOAD_DIR

# Install app-level dependencies
COPY --chown=node:node ./package.json  ./package-lock.json /home/node/app/
RUN npm install --production

# Install application code by copy-pasting the source to the image
# (subject to .dockerignore)
COPY --chown=node:node ./scripts/ $WORKDIR/scripts/
COPY --chown=node:node ./lib $WORKDIR/lib/
COPY --chown=node:node ./index.js $WORKDIR

# Store reference to commit in version control system in image
ARG VCS_REF
LABEL org.label-schema.vcs-ref=$VCS_REF

# Unless overridden, run this command upon instantiation
ENTRYPOINT [ "node", "index.js" ]
