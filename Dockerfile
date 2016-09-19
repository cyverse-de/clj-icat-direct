FROM clojure
COPY ./docker/profiles.clj /root/.lein/profiles.clj
WORKDIR /usr/src/clj-icat-direct

COPY project.clj /usr/src/clj-icat-direct/
RUN lein deps

COPY . /usr/src/clj-icat-direct
CMD ["lein", "test"]
