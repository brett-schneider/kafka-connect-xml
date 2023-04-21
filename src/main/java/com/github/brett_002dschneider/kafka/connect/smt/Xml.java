package com.github.brett_002dschneider.kafka.connect.smt;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
// import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.Map;

import java.lang.String;

// import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
// import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
// import com.fasterxml.jackson.databind.JsonMappingException;
// import com.fasterxml.jackson.databind.JsonProcessingException;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Xml<R extends ConnectRecord<R>> implements Transformation<R> {
  
  // public String proc(String xmlString) throws java.io.IOException{
  // this.xmlMapper = new XmlMapper();
  // this.jsonMapper = new ObjectMapper();    
  // return this.jsonMapper.writeValueAsString(this.xmlMapper.readTree(xmlString.getBytes()));

  Logger logger = LoggerFactory.getLogger(Xml.class);

  public static final String OVERVIEW_DOC =
    "Process XML from String in Kafka Connect Transform";

  // private interface ConfigName {
  //   String UUID_FIELD_NAME = "uuid.field.name";
  // }
 
  // public static final ConfigDef CONFIG_DEF = new ConfigDef()
  //   .define(ConfigName.UUID_FIELD_NAME, ConfigDef.Type.STRING, "uuid", ConfigDef.Importance.HIGH,
  //     "Field name for UUID");

  public static final ConfigDef CONFIG_DEF = new ConfigDef();

  private static final String PURPOSE = "XML Transform";

  private static final XmlMapper xmlMapper = new XmlMapper();
  private static final ObjectMapper objectMapper = new ObjectMapper();

  // private String fieldName;

  private Cache<Schema, Schema> schemaUpdateCache;

  @Override
  public void configure(Map<String, ?> props) {
    // final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    // fieldName = config.getString(ConfigName.UUID_FIELD_NAME);

    schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
  }


  @Override
  public R apply(R record) {
    // logger.info("apply operatingSchema(record): {}", operatingSchema(record));
    // logger.info("apply operatingValue(record): {}", operatingValue(record));
    // logger.info("apply record: {}", record);
    final Map<String, Object> value = convert(record.value().toString());
    final Map<String, Object> updatedValue = new HashMap<>(value);
    return newRecord(record, null, updatedValue);
}

  private Map<String, Object> convert(String str) {
    try {
      JsonNode node = Xml.xmlMapper.readTree(str);
      Map<String, Object> value = Xml.objectMapper.convertValue(node, new TypeReference<Map<String, Object>>(){});
      return value;
      // return newRecord(record, null, value);
    }
    catch(IOException e) {
      return null;
    }
  }

  // private R applySchemaless(R record) {
  //   final Map<String, Object> value = convert(record.value().toString());
  //   final Map<String, Object> updatedValue = new HashMap<>(value);
  //   return newRecord(record, null, updatedValue);
  // }
 
  // private R applySchemaless(R record) {
  //   final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);

  //   final Map<String, Object> updatedValue = new HashMap<>(value);

  //   updatedValue.put(fieldName, getRandomUuid());

  //   return newRecord(record, null, updatedValue);
  // }
 
  // private R applyWithSchema(R record) {
  //   if (operatingSchema(record) == Schema.STRING_SCHEMA) {
  //       final Map<String, Object> value = convert(record.value().toString());
  //       final Map<String, Object> updatedValue = new HashMap<>(value);
  //       return newRecord(record, null, updatedValue);
  //   } else {
  //     final Struct value = requireStruct(operatingValue(record), PURPOSE);
    
  //     Schema updatedSchema = schemaUpdateCache.get(value.schema());
  //     if(updatedSchema == null) {
  //       updatedSchema = makeUpdatedSchema(value.schema());
  //       schemaUpdateCache.put(value.schema(), updatedSchema);
  //     }
      
  //     final Struct updatedValue = new Struct(updatedSchema);
      
  //     for (Field field : value.schema().fields()) {
  //         updatedValue.put(field.name(), value.get(field));
  //     }
                
  //     return newRecord(record, updatedSchema, updatedValue);
  //  }
  // }

  // private R applyWithSchema(R record) {
  //     final Struct value = requireStruct(operatingValue(record), PURPOSE);
    
  //     Schema updatedSchema = schemaUpdateCache.get(value.schema());
  //     if(updatedSchema == null) {
  //       updatedSchema = makeUpdatedSchema(value.schema());
  //       schemaUpdateCache.put(value.schema(), updatedSchema);
  //     }
      
  //     final Struct updatedValue = new Struct(updatedSchema);
      
  //     for (Field field : value.schema().fields()) {
  //         updatedValue.put(field.name(), value.get(field));
  //     }
                
  //     return newRecord(record, updatedSchema, updatedValue);
  //   }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
    schemaUpdateCache = null;
  }

  private Schema makeUpdatedSchema(Schema schema) {
    final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

    for (Field field: schema.fields()) {
      builder.field(field.name(), field.schema());
    }

    // builder.field(fieldName, Schema.STRING_SCHEMA);

    return builder.build();
  }

  protected abstract Schema operatingSchema(R record);

  protected abstract Object operatingValue(R record);

  protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

  public static class Key<R extends ConnectRecord<R>> extends Xml<R> {

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

  public static class Value<R extends ConnectRecord<R>> extends Xml<R> {

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


