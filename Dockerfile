FROM amazonlinux:2

RUN yum install -y tar wget gzip make


RUN wget https://go.dev/dl/go1.24.0.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go1.24.0.linux-amd64.tar.gz && \
    rm go1.24.0.linux-amd64.tar.gz
ENV PATH=$PATH:/usr/local/go/bin

RUN wget -q https://repo1.maven.org/maven2/org/flywaydb/flyway-commandline/9.16.1/flyway-commandline-9.16.1-linux-x64.tar.gz && \
    tar -xzf flyway-commandline-9.16.1-linux-x64.tar.gz && \
    mv flyway-9.16.1 flyway && \
    ln -s /flyway/flyway /usr/local/bin/flyway && \
    rm flyway-commandline-9.16.1-linux-x64.tar.gz

WORKDIR /app

COPY ./datastore .
COPY ./.env .
COPY ./internal/db/schema/ ./internal/db/schema/
COPY ./Makefile .

EXPOSE 8080

CMD ["./datastore"]
