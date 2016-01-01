package org.apache.kafka.connect.json;

import java.util.Collections;

import org.junit.Before;

public class JsonConverterTest extends AbstractJsonConverterTest {
    @Before
    public void setUp() {
        converter.configure(Collections.emptyMap(), false);
    }
}
