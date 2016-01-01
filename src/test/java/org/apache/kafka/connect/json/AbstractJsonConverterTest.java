package org.apache.kafka.connect.json;

import static org.junit.Assert.assertEquals;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class AbstractJsonConverterTest {

    protected ObjectMapper objectMapper = new ObjectMapper();
    protected JsonConverter converter = new JsonConverter();

    @Test
    public void testJsonConverter() throws Exception {
        //@formatter:off
        Schema schema = SchemaBuilder.struct()
                                     .name("Person")
                                     .field("firstName", Schema.STRING_SCHEMA)
                                     .field("lastName", Schema.STRING_SCHEMA)
                                     .field("email", Schema.STRING_SCHEMA)
                                     .field("age", Schema.INT32_SCHEMA)
                                     .field("weightInKgs", Schema.INT32_SCHEMA)
                                     .build();

        Struct cartman = new Struct(schema)
                             .put("firstName", "Eric")
                             .put("lastName", "Cartman")
                             .put("email", "eric.cartman@southpark.com")
                             .put("age", 10)
                             .put("weightInKgs", 40);
        //@formatter:on

        byte[] cartmanSerialized = converter.fromConnectData("southpark", schema, cartman);
        SchemaAndValue connectData = converter.toConnectData("southpark", cartmanSerialized);
                
        Schema schemaFromConnectData = connectData.schema();
        Object valueFromConnectData = connectData.value();
        
        assertEquals(schema, schemaFromConnectData);
        assertEquals(cartman, valueFromConnectData);
    }
}
