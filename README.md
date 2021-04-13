# clj-icat-direct

A Clojure library for directly connecting to the iRODS ICAT database and running queries against it. Uses Korma to
connect, which might be overkill right now, but provides connection-pooling and a way forward from simply running
strings containing SQL against the ICAT.

See the LICENSE file for licensing information.

## Testing from the REPL

This library is configured to add the path `./repl` to the source file search path in REPL mode, so that developers can
easily test using the Clojure REPL. To use this feature, first create two files: `$HOME/.irods/.qa-db.json` for QA, and
`$HOME/.irods/.prod-db.json` for production. Note that these files will contain sensitive connection information for the
ICAT database. Be sure to set the file permissions accordingly. Each file should look like this:

``` json
{
    "host": "icat.example.org",
    "port": 5432,
    "user": "somedbuser",
    "password": "somedbpassword"
}
```

Once you have the files in place, you can easily configure the REPL to access the ICAT database by calling `init-prod`
or `init-qa` from the `clj-icat-direct.repl-utils` namespace:

```
user=> (require '[clj-icat-direct.repl-utils :as ru])
nil

user=> (ru/init-prod)
;; output omitted
```

After configuring the REPL, you can other functions in `clj-icat-direct` as usual:

```
user=> (require '[clj-icat-direct.icat :as icat])
nil

user=> (icat/number-of-items-in-folder "a" "example" "/example/home/a" :any)
25
```

The `repl` directory is also included in `.gitignore` so that you can add additional namespaces to it without having to
worry very much about accidentally including them in a commit. This can be very helpful if you have an editor that can
interactively run Clojure code.
