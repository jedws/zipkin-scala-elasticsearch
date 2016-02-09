#!/usr/bin/env bash

set -e

V=$(git rev-parse --short HEAD)

./gradlew :zipkin-laas:shadowJar $*

rm -rf docker-build
mkdir docker-build

cp zipkin-laas/build/libs/zipkin-laas-$V-all.jar docker-build

cp -r zipkin-web/src/main/resources docker-build

cat > docker-build/Dockerfile <<- EOF
FROM java:8

COPY . /tmp
WORKDIR /tmp

CMD ["java", "-jar", "zipkin-laas-$V-all.jar", "-zipkin.web.resourcesRoot=/tmp/resources"]
EOF

docker build -t zipkin-laas docker-build
