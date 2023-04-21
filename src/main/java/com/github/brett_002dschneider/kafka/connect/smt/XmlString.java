package com.github.brett_002dschneider.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;

import org.apache.kafka.connect.transforms.Transformation;

import java.util.HashMap;
import java.util.Map;

import java.lang.String;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class XmlString<R extends ConnectRecord<R>> implements Transformation<R> {
  
  Logger logger = LoggerFactory.getLogger(Xml.class);

  public static final String OVERVIEW_DOC =
    "Process XML from String in Kafka Connect Transform";

  public static final ConfigDef CONFIG_DEF = new ConfigDef();

  // private static final String PURPOSE = "XML Transform";

  private static final XmlMapper xmlMapper = new XmlMapper();
  private static final ObjectMapper objectMapper = new ObjectMapper();


  @Override
  public void configure(Map<String, ?> props) {
  }


  @Override
  public R apply(R record) {
    logger.debug("apply operatingSchema(record): {}", operatingSchema(record));
    logger.debug("apply operatingValue(record): {}", operatingValue(record));
    logger.debug("apply record: {}", record);
    final Map<String, Object> value = convert(record.value().toString());
    final Map<String, Object> updatedValue = new HashMap<>(value);
    return newRecord(record, null, updatedValue);
}

  private Map<String, Object> convert(String str) {
    try {
      JsonNode node = XmlString.xmlMapper.readTree(str);
      Map<String, Object> value = XmlString.objectMapper.convertValue(node, new TypeReference<Map<String, Object>>(){});
      return value;
    }
    catch(IOException e) {
      return null;
    }
  }


  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
  }

  protected abstract Schema operatingSchema(R record);

  protected abstract Object operatingValue(R record);

  protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

  public static class Key<R extends ConnectRecord<R>> extends XmlString<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.keySchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.key();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
    }

  }

  public static class Value<R extends ConnectRecord<R>> extends XmlString<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.value();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
    }

  }
}


