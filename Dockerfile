FROM clojure
RUN apt-get update && apt-get install -y git

COPY ./docker/profiles.clj /root/.lein/profiles.clj
WORKDIR /usr/src/app

COPY project.clj /usr/src/app/
RUN lein deps

COPY . /usr/src/app
CMD ["lein", "test2junit"]
