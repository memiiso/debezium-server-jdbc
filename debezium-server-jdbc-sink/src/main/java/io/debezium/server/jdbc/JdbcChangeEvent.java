/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.jdbc;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import org.jdbi.v3.core.qualifier.QualifiedType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ismail Simsek
 */
public class JdbcChangeEvent {

  protected static final Logger LOGGER = LoggerFactory.getLogger(JdbcChangeEvent.class);
  protected final String destination;
  protected final JsonNode value;
  protected final JsonNode key;
  Schema schema;

  public JdbcChangeEvent(String destination, JsonNode value, JsonNode key, JsonNode valueSchema, JsonNode keySchema) {
    this.destination = destination;
    this.value = value;
    this.key = key;
    this.schema = new Schema(valueSchema, keySchema);
  }

  public JsonNode key() {
    return key;
  }

  public JsonNode value() {
    return value;
  }

  public Map<String, Object> valueAsMap() {
    return JdbcChangeConsumer.mapper.convertValue(value(), new TypeReference<>() {
    });
  }

  public Map<String, Object> keyAsMap() {
    return JdbcChangeConsumer.mapper.convertValue(key(), new TypeReference<>() {
    });
  }

  public String operation() {
    return value().get("__op").textValue();
  }

  public Schema schema() {
    return schema;
  }

  public String destination() {
    return destination;
  }

//  public GenericRecord asIcebergRecord(Schema schema) {
//    final GenericRecord record = asIcebergRecord(schema.asStruct(), value);
//
//    if (value != null && value.has("__source_ts_ms") && value.get("__source_ts_ms") != null) {
//      final long source_ts_ms = value.get("__source_ts_ms").longValue();
//      final OffsetDateTime odt = OffsetDateTime.ofInstant(Instant.ofEpochMilli(source_ts_ms), ZoneOffset.UTC);
//    return record;
//  }

//  private GenericRecord asIcebergRecord(Types.StructType tableFields, JsonNode data) {
//    LOGGER.debug("Processing nested field:{}", tableFields);
//    GenericRecord record = GenericRecord.create(tableFields);
//
//    for (Types.NestedField field : tableFields.fields()) {
//      // Set value to null if json event don't have the field
//      if (data == null || !data.has(field.name()) || data.get(field.name()) == null) {
//        record.setField(field.name(), null);
//        continue;
//      }
//      // get the value of the field from json event, map it to iceberg value
//      record.setField(field.name(), jsonValToIcebergVal(field, data.get(field.name())));
//    }
//
//    return record;
//  }

//  private Object jsonValToIcebergVal(Types.NestedField field, JsonNode node) {
//    LOGGER.debug("Processing Field:{} Type:{}", field.name(), field.type());
//    final Object val;
//    switch (field.type().typeId()) {
//      case INTEGER: // int 4 bytes
//        val = node.isNull() ? null : node.asInt();
//        break;
//      case LONG: // long 8 bytes
//        val = node.isNull() ? null : node.asLong();
//        break;
//      case FLOAT: // float is represented in 32 bits,
//        val = node.isNull() ? null : node.floatValue();
//        break;
//      case DOUBLE: // double is represented in 64 bits
//        val = node.isNull() ? null : node.asDouble();
//        break;
//      case BOOLEAN:
//        val = node.isNull() ? null : node.asBoolean();
//        break;
//      case STRING:
//        // if the node is not a value node (method isValueNode returns false), convert it to string.
//        val = node.isValueNode() ? node.asText(null) : node.toString();
//        break;
//      case UUID:
//        val = node.isValueNode() ? UUID.fromString(node.asText(null)) : UUID.fromString(node.toString());
//        break;
//      case BINARY:
//        try {
//          val = node.isNull() ? null : ByteBuffer.wrap(node.binaryValue());
//        } catch (IOException e) {
//          throw new RuntimeException("Failed to convert binary value to iceberg value, field: " + field.name(), e);
//        }
//        break;
//      case LIST:
//        val = JdbcChangeConsumer.mapper.convertValue(node, ArrayList.class);
//        break;
//      case MAP:
//        val = JdbcChangeConsumer.mapper.convertValue(node, Map.class);
//        break;
//      case STRUCT:
//        // create it as struct, nested type
//        // recursive call to get nested data/record
//        val = asIcebergRecord(field.type().asStructType(), node);
//        break;
//      default:
//        // default to String type
//        // if the node is not a value node (method isValueNode returns false), convert it to string.
//        val = node.isValueNode() ? node.asText(null) : node.toString();
//        break;
//    }
//
//    return val;
//  }

  public static class Schema {
    private final JsonNode valueSchema;
    private final JsonNode keySchema;

    Schema(JsonNode valueSchema, JsonNode keySchema) {
      this.valueSchema = valueSchema;
      this.keySchema = keySchema;
    }

    public JsonNode valueSchema() {
      return valueSchema;
    }

    public JsonNode keySchema() {
      return keySchema;
    }

    public Map<String, QualifiedType<?>> valueSchemaFields() {
      if (valueSchema != null && valueSchema.has("fields") && valueSchema.get("fields").isArray()) {
        LOGGER.debug(valueSchema.toString());
        return fields(valueSchema, "", 0);
      }
      LOGGER.trace("Event schema not found!");
      return new HashMap<>();
    }

    public Map<String, QualifiedType<?>> keySchemaFields() {
      if (keySchema != null && keySchema.has("fields") && keySchema.get("fields").isArray()) {
        LOGGER.debug(keySchema.toString());
        return fields(keySchema, "", 0);
      }
      LOGGER.trace("Key schema not found!");
      return new HashMap<>();
    }

    private Map<String, QualifiedType<?>> fields(JsonNode eventSchema, String schemaName, int columnId) {
      Map<String, QualifiedType<?>> fields = new HashMap<>();
      String schemaType = eventSchema.get("type").textValue();
      LOGGER.debug("Converting Schema of: {}::{}", schemaName, schemaType);
      for (JsonNode jsonSchemaFieldNode : eventSchema.get("fields")) {
        columnId++;
        String fieldName = jsonSchemaFieldNode.get("field").textValue();
        String fieldType = jsonSchemaFieldNode.get("type").textValue();
        LOGGER.debug("Processing Field: [{}] {}.{}::{}", columnId, schemaName, fieldName, fieldType);
        switch (fieldType) {
          case "array":
            JsonNode items = jsonSchemaFieldNode.get("items");
            if (items != null && items.has("type")) {
              fields.put(fieldName, QualifiedType.of(String.class));
//              String listItemType = items.get("type").textValue();
//
//              if (listItemType.equals("struct") || listItemType.equals("array") || listItemType.equals("map")) {
//                throw new RuntimeException("Complex nested array types are not supported," + " array[" + listItemType + "], field " + fieldName);
//              }
//              QualifiedType<?> item = icebergFieldType(listItemType);
//              fields.put(fieldName, QualifiedType.of(?????));
            } else {
              throw new RuntimeException("Unexpected Array type for field " + fieldName);
            }
            break;
          case "map":
            fields.put(fieldName, QualifiedType.of(String.class));
            //fields.put(fieldName, QualifiedType.of(Map.class));
            //throw new RuntimeException("'" + fieldName + "' has Map type, Map type not supported!");
            break;
          case "struct":
            // create it as struct, nested type
            fields.put(fieldName, QualifiedType.of(String.class));
            //Map<String, QualifiedType<?>> subSchema = fields(jsonSchemaFieldNode, fieldName, columnId);
            //fields.put(fieldName, subSchema);
            break;
          default: //primitive types
            fields.put(fieldName, fieldType(fieldType));
            break;
        }
      }

      return fields;
    }

    private QualifiedType<?> fieldType(String fieldType) {
      switch (fieldType) {
        case "int8":
        case "int16":
        case "int32": // int 4 bytes
          return QualifiedType.of(Integer.class);
        case "int64": // long 8 bytes
          return QualifiedType.of(Long.class);
        case "float8":
        case "float16":
        case "float32": // float is represented in 32 bits,
          return QualifiedType.of(Float.class);
        case "float64": // double is represented in 64 bits
          return QualifiedType.of(Double.class);
        case "boolean":
          return QualifiedType.of(Boolean.class);
        case "string":
          return QualifiedType.of(String.class);
        case "uuid":
          // @TODO is it better to UUID instead?
          return QualifiedType.of(String.class);
        case "bytes":
          return QualifiedType.of(Byte.class);
        default:
          // default to String type
          return QualifiedType.of(String.class);
        //throw new RuntimeException("'" + fieldName + "' has "+fieldType+" type, "+fieldType+" not supported!");
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Schema that = (Schema) o;
      return Objects.equals(valueSchema, that.valueSchema) && Objects.equals(keySchema, that.keySchema);
    }

    @Override
    public int hashCode() {
      return Objects.hash(valueSchema, keySchema);
    }
  }

}
