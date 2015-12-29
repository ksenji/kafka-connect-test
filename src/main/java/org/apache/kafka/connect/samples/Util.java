package org.apache.kafka.connect.samples;

import java.lang.reflect.Field;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.samples.pojos.Pojo;

public class Util {

    private static Map<Class<?>, Schema> MAPPING = new HashMap<>();

    public static URL resource(String name) {
        return Thread.currentThread().getContextClassLoader().getResource(name);
    }

    public static Schema schema() {
        return SchemaHolder.SCHEMA;
    }

    public static Struct wrap(Pojo pojo) {
        Struct struct = new Struct(schema());
        if (pojo != null) {
            Field[] fields = Pojo.class.getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                try {
                    struct.put(field.getName(), field.get(pojo));
                } catch (IllegalArgumentException | IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        }
        return struct;
    }

    public static Pojo unwrap(Struct struct) {
        Pojo pojo = new Pojo();
        if (struct != null) {
            Field[] fields = Pojo.class.getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                try {
                    field.set(pojo, struct.get(field.getName()));
                } catch (IllegalArgumentException | IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        }
        return pojo;
    }

    public static JsonConverter convertor() {
        return JsonConverterHolder.CONVERTER;
    }

    private static final class SchemaHolder {
        static {
            // INT8, INT16, INT32, INT64, FLOAT32, FLOAT64, BOOLEAN, STRING,
            // BYTES,
            // ARRAY, MAP, STRUCT;
            MAPPING.put(Short.class, Schema.INT8_SCHEMA);
            MAPPING.put(Short.TYPE, Schema.INT8_SCHEMA);
            MAPPING.put(Character.class, Schema.INT16_SCHEMA);
            MAPPING.put(Character.TYPE, Schema.INT16_SCHEMA);
            MAPPING.put(Integer.class, Schema.INT32_SCHEMA);
            MAPPING.put(Integer.TYPE, Schema.INT32_SCHEMA);
            MAPPING.put(Long.class, Schema.INT64_SCHEMA);
            MAPPING.put(Long.TYPE, Schema.INT64_SCHEMA);
            MAPPING.put(Float.class, Schema.FLOAT32_SCHEMA);
            MAPPING.put(Float.TYPE, Schema.FLOAT32_SCHEMA);
            MAPPING.put(Double.class, Schema.FLOAT64_SCHEMA);
            MAPPING.put(Double.TYPE, Schema.FLOAT64_SCHEMA);
            MAPPING.put(Boolean.class, Schema.BOOLEAN_SCHEMA);
            MAPPING.put(Boolean.TYPE, Schema.BOOLEAN_SCHEMA);
            MAPPING.put(String.class, Schema.STRING_SCHEMA);
            MAPPING.put(Byte.class, Schema.BYTES_SCHEMA);
            MAPPING.put(Byte.TYPE, Schema.BYTES_SCHEMA);
        }

        private static final Schema SCHEMA = initSchema();

        private static Schema initSchema() {
            SchemaBuilder builder = SchemaBuilder.struct().name(Pojo.class.getName()).version(1).doc("A Pojo class representing a Person");
            Field[] fields = Pojo.class.getDeclaredFields();
            for (Field field : fields) {
                builder = builder.field(field.getName(), getSchema(field));
            }

            return builder.build();
        }

        private static Schema getSchema(Field f) {
            Class<?> klass = f.getType();
            Schema schema = MAPPING.get(klass);
            assert schema != null;
            return schema;
        }
    }

    private static final class JsonConverterHolder {
        private static final JsonConverter CONVERTER = initConverter();

        private static JsonConverter initConverter() {
            JsonConverter converter = new JsonConverter();
            converter.configure(Collections.emptyMap(), false);
            return converter;
        }
    }

}
