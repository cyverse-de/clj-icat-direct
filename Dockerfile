FROM clojure
COPY . /usr/src/clj-icat-direct
COPY ./docker/profiles.clj /root/.lein/profiles.clj
WORKDIR /usr/src/clj-icat-direct
RUN lein deps
CMD ["lein", "test"]
