FROM node:17-alpine
ARG JSP2PD_VERSION

RUN npm install --global libp2p-daemon@${JSP2PD_VERSION}

USER node

ENV PRIVATE_KEY_FILE=""
ENV LISTEN_MADDR=/ip4/0.0.0.0/tcp/4024
ENV HOST_MADDRS=/ip4/0.0.0.0/tcp/4025
ENV PUBSUB=false
ENV PUBSUB_ROUTER=gossipsub

ENTRYPOINT jsp2pd --id ${PRIVATE_KEY_FILE} --listen=${LISTEN_MADDR} --hostAddrs=${HOST_MADDRS} --pubsub=${PUBSUB} --pubsubRouter=${PUBSUB_ROUTER}
