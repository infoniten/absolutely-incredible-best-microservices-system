FROM postgres:16-alpine

# Copy init scripts (executed in alphabetical order)
COPY 00-create-databases.sh /docker-entrypoint-initdb.d/
COPY init.sql /docker-entrypoint-initdb.d/01-init.sql
RUN chmod 755 /docker-entrypoint-initdb.d/00-create-databases.sh && \
    chmod 644 /docker-entrypoint-initdb.d/01-init.sql

ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres
ENV POSTGRES_DB=quantara

EXPOSE 5432
