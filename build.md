# build 

## [jdk12](https://www.oracle.com/java/technologies/javase/jdk12-archive-downloads.html)

## idea setting

* jvm setting

```
-Des.path.conf=./config
-Djava.security.policy=./config\java.policy
-Des.path.home=./data
-Dlog4j2.disable.jmx=true
```

* [x] include dependencies with `Provided` scope

## run

> org.elasticsearch.bootstrap.Elasticsearch