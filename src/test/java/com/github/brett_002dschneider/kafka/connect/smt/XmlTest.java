/*
 * Copyright © 2019 Christopher Matta (chris.matta@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.brett_002dschneider.kafka.connect.smt;

// import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import com.github.brett_002dschneider.kafka.connect.smt.Xml;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class XmlTest {

  private Xml<SourceRecord> xform = new Xml.Value<>();

  @After
  public void tearDown() throws Exception {
    xform.close();
  }

  // @Test(expected = DataException.class)
  // public void topLevelStructRequired() {
  //   xform.apply(new SourceRecord(null, null, "", 0, Schema.INT32_SCHEMA, 42));
  //   // apply operatingSchema(record): Schema{INT32}
  //   // SourceRecord{sourcePartition=null, sourceOffset=null} 
  //   //  ConnectRecord{topic='', kafkaPartition=0, key=null, keySchema=null, value=42, valueSchema=Schema{INT32}, timestamp=null, headers=ConnectHeaders(headers=)}
  // }

  // @Test
  // public void copySchemaXml() {
  //   final Map<String, Object> props = new HashMap<>();

  //   xform.configure(props);

  //   final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("magic", Schema.OPTIONAL_INT64_SCHEMA).build();
  //   final Struct simpleStruct = new Struct(simpleStructSchema).put("magic", 42L);

  //   final SourceRecord record = new SourceRecord(null, null, "test", 0, simpleStructSchema, simpleStruct);
  //   final SourceRecord transformedRecord = xform.apply(record);
  //   // operatingSchema(record): Schema{name:STRUCT}
  //   // SourceRecord{sourcePartition=null, sourceOffset=null} 
  //   //  ConnectRecord{topic='test', kafkaPartition=0, key=null, keySchema=null, value=Struct{magic=42}, valueSchema=Schema{name:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}

  //   assertEquals(simpleStructSchema.name(), transformedRecord.valueSchema().name());
  //   assertEquals(simpleStructSchema.version(), transformedRecord.valueSchema().version());
  //   assertEquals(simpleStructSchema.doc(), transformedRecord.valueSchema().doc());

  //   assertEquals(Schema.OPTIONAL_INT64_SCHEMA, transformedRecord.valueSchema().field("magic").schema());
  //   assertEquals(42L, ((Struct) transformedRecord.value()).getInt64("magic").longValue());
  //   // assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema().field("myUuid").schema());
  //   // assertNotNull(((Struct) transformedRecord.value()).getString("myUuid"));

  //   // Exercise caching
  //   final SourceRecord transformedRecord2 = xform.apply(
  //     new SourceRecord(null, null, "test", 1, simpleStructSchema, new Struct(simpleStructSchema)));
  //   assertSame(transformedRecord.valueSchema(), transformedRecord2.valueSchema());
  //   // operatingSchema(record): Schema{name:STRUCT}
  //   // SourceRecord{sourcePartition=null, sourceOffset=null}
  //   //  ConnectRecord{topic='test', kafkaPartition=1, key=null, keySchema=null, value=Struct{}, valueSchema=Schema{name:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}

  // }

  @Test
  public void schemalessXml() {
    final Map<String, Object> props = new HashMap<>();

    xform.configure(props);

    final SourceRecord record = new SourceRecord(null, null, "test", 0,
      null, "<xml><magic>42</magic></xml>");

    final SourceRecord transformedRecord = xform.apply(record);
    assertEquals("42", ((Map) transformedRecord.value()).get("magic"));
    // assertNotNull(((Map) transformedRecord.value()).get("myUuid"));

  }

  // @Test
  // public void schemaXml() {
  //   final Map<String, Object> props = new HashMap<>();

  //   xform.configure(props);

  //   final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("magic", Schema.STRING_SCHEMA).build();
  //   final Struct simpleStruct = new Struct(simpleStructSchema).put("magic", "<?xml version=\"1.0\"?><PurchaseOrder PurchaseOrderNumber=\"99503\" OrderDate=\"1999-10-20\"><Address Type=\"Shipping\"><Name>Ellen Adams</Name><Street>123 Maple Street</Street><City>Mill Valley</City><State>CA</State><Zip>10999</Zip><Country>USA</Country></Address><Address Type=\"Billing\"><Name>Tai Yee</Name><Street>8 Oak Avenue</Street><City>Old Town</City><State>PA</State><Zip>95819</Zip><Country>USA</Country></Address><DeliveryNotes>Please leave packages in shed by driveway.</DeliveryNotes><Items><Item PartNumber=\"872-AA\"><ProductName>Lawnmower</ProductName><Quantity>1</Quantity><USPrice>148.95</USPrice><Comment>Confirm this is electric</Comment></Item><Item PartNumber=\"926-AA\"><ProductName>Baby Monitor</ProductName><Quantity>2</Quantity><USPrice>39.98</USPrice><ShipDate>1999-05-21</ShipDate></Item></Items></PurchaseOrder>");

  //   final SourceRecord record = new SourceRecord(null, null, "test", 0, simpleStructSchema, simpleStruct);
  //   final SourceRecord transformedRecord = xform.apply(record);

  //   assertEquals(simpleStructSchema.name(), transformedRecord.valueSchema().name());
  //   assertEquals(simpleStructSchema.version(), transformedRecord.valueSchema().version());
  //   assertEquals(simpleStructSchema.doc(), transformedRecord.valueSchema().doc());

  //   assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema().field("magic").schema());
  //   // assertEquals(42L, ((Struct) transformedRecord.value()).getInt64("magic").longValue());

  // }

}