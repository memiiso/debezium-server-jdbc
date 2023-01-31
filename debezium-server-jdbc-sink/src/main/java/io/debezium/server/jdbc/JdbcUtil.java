/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.jdbc;

import io.debezium.DebeziumException;
import io.debezium.server.jdbc.querydsl.ddl.CreateTableClause;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.literal.NamedLiteral;

import com.fasterxml.jackson.databind.JsonNode;
import com.querydsl.sql.Configuration;
import com.querydsl.sql.SQLTemplatesRegistry;
import org.eclipse.microprofile.config.Config;
import org.jdbi.v3.core.qualifier.QualifiedType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcChangeConsumer.class);

  public static <T> T selectInstance(Instance<T> instances, String name) {

    Instance<T> instance = instances.select(NamedLiteral.of(name));
    if (instance.isAmbiguous()) {
      throw new DebeziumException("Multiple batch size wait class named '" + name + "' were found");
    } else if (instance.isUnsatisfied()) {
      throw new DebeziumException("No batch size wait class named '" + name + "' is available");
    }

    LOGGER.info("Using {}", instance.getClass().getName());
    return instance.get();
  }

  public static Map<String, String> getConfigSubset(Config config, String prefix) {
    final Map<String, String> ret = new HashMap<>();

    for (String propName : config.getPropertyNames()) {
      if (propName.startsWith(prefix)) {
        final String newPropName = propName.substring(prefix.length());
        ret.put(newPropName, config.getValue(propName, String.class));
      }
    }

    return ret;
  }

  private static Map<String, Class<?>> fieldsV2(JsonNode valueSchema) {
    if (valueSchema != null && valueSchema.has("fields") && valueSchema.get("fields").isArray()) {
      return fieldsV2(valueSchema, "", 0);
    }
    return new HashMap<>();
  }

  private static Map<String, Class<?>> fieldsV2(JsonNode eventSchema, String schemaName, int columnId) {
    Map<String, Class<?>> fields = new HashMap<>();
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
            fields.put(fieldName, String.class);// java.sql.Array
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
          fields.put(fieldName, String.class);
          //fields.put(fieldName, QualifiedType.of(Map.class));
          //throw new RuntimeException("'" + fieldName + "' has Map type, Map type not supported!");
          break;
        case "struct":
          // create it as struct, nested type
          fields.put(fieldName, String.class);
          //Map<String, QualifiedType<?>> subSchema = fields(jsonSchemaFieldNode, fieldName, columnId);
          //fields.put(fieldName, subSchema);
          break;
        default: //primitive types
          fields.put(fieldName, fieldTypeV2(fieldType));
          break;
      }
    }

    return fields;
  }

  private static Class<?> fieldTypeV2(String fieldType) {
    switch (fieldType) {
      case "int8":
      case "int16":
      case "int32": // int 4 bytes
        return long.class;
      case "int64": // long 8 bytes
        return long.class;
      case "float8":
      case "float16":
      case "float32": // float is represented in 32 bits,
        return double.class;
      case "float64": // double is represented in 64 bits
        return double.class;
      case "boolean":
        return boolean.class;
      case "string":
        return String.class;
      case "uuid":
        return String.class;
      case "bytes":
        return byte.class;
      default:
        // default to String type
        return String.class;
      //throw new RuntimeException("'" + fieldName + "' has "+fieldType+" type, "+fieldType+" not supported!");
    }
  }


  public static void createTable(String schemaName, String tableName, Connection conn, JdbcChangeEvent.Schema schema,
                                 boolean upsert) {
    try {
      SQLTemplatesRegistry tr = new SQLTemplatesRegistry();
      Configuration configuration = new Configuration(tr.getTemplates(conn.getMetaData()));
      String tableNameWithSchema = String.format("%s.%s", schemaName, tableName);
      CreateTableClause ct = new CreateTableClause(conn, configuration, tableNameWithSchema);

      Map<String, Class<?>> fieldv2 = JdbcUtil.fieldsV2(schema.valueSchema());

      fieldv2.forEach(ct::column);

      Map<String, QualifiedType<?>> SchemaPKFields = schema.keySchemaFields();
      if (!SchemaPKFields.isEmpty()) {
        ct.primaryKey(String.format("%s", tableName), SchemaPKFields.keySet().toArray(new String[0]));
      }

      ct.execute();
    } catch (SQLException e) {
      throw new DebeziumException("Failed to create table", e);
    }
  }

}