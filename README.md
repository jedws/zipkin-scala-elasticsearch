# zipkin-laas #

Atlassian specific customisations and wrapper app around [Zipkin](https://github.com/openzipkin/zipkin):

  * [go/zipkin](http://go.atlassian.com/zipkin) or [zipkin.atlassian.io](https://zipkin.atlassian.io)

Currently, it contains:

* `zipkin-elasticsearch`: Implementation of Zipkin's `SpanStore` that hooks up with ElasticSearch
* `zipkin-laas`: LaaS-specific configuration/code, and the main application that spins up `zipkin-query` and `zipkin-web`.
* `zipkin-web`: Slightly customised version of Zipkin's `zipkin-web` modules, primarily because we need the web resources local to the server when running the app.

### Getting started ###

To build:

    ./build.sh

To run:

    LAAS_USER=(LDAP username) LAAS_PASS=(LDAP paasword) ./bin/laas

Then you should have access via `http://localhost:8080`

Standard Gradle commands should work as well e.g. `./gradlew test` to run all the tests.

### Adding new services ###
If the service is on Micros, and is using the standard structure (Link TBC), then you should just need to modify `Main.scala` in `zipkin-laas` to add your service and the environments you want to enable Zipkin in.

If the trace information is in a non-standard structure, you can add a customised `IndexPattern`, see `com.twitter.zipkin.laas.Blobstore` as an example.

The basic format is:
```
{
    "trace": {
        "trace_id": ...,
        "name": ..., // string, name of span, e.g rpc method
        "span_id": ...,
        "parent_span_id": ..., // optional
        "type": ..., // either "client" or "server"
        "recv_timestamp": ..., // optional, milliseconds from epoch
        "send_timestamp": ... // optional, milliseconds from epoch
    }
}
```

There is also an early version of a log parser for Unicorn apps, which follows the same structure as above, except namespaced under `ext` to align with `atlassian-logging`.

Be sure to add tests in `zipkin-laas` (we're using Specs2 for these tests).

### Running a Scala REPL ###

The `repl` project is configured to run a REPL importing all the code from `zipkin-laas` main. To run the repl, run `repl.sh` in the project root directory. It also references a `repl.scala` file that can contain initial commands (i.e. `initialCommands` in SBT)

### Resources ###
* [JIRA project](https://sdog.jira.com/secure/RapidBoard.jspa?rapidView=170)
* [Builds](https://collaboration-bamboo.internal.atlassian.com/browse/FIL-ZKB)
    * [Build plan templates](https://stash.atlassian.com/projects/ES/repos/zipkin-build)


### Who do I talk to? ###

* Zipkin Hipchat room
* Sam Reis
* Sidney Shek
