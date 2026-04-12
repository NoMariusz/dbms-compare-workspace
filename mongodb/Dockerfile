FROM mongo:8.2

# Required when running a replica set with authorization enabled.
COPY mongo-keyfile /etc/mongo-keyfile
RUN chmod 600 /etc/mongo-keyfile && chown mongodb:mongodb /etc/mongo-keyfile