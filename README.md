Kafka Connect SMT to parse String as XML

Example on how to add to your connector:
```
transforms=toxml
transforms.toxml.type=com.github.brett_002dschneider.kafka.connect.smt.Xml$Value
```

Based on https://github.com/cjmatta/kafka-connect-insert-uuid. Thanks!