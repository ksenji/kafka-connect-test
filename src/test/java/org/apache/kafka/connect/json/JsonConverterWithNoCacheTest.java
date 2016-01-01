package org.apache.kafka.connect.json;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;

public class JsonConverterWithNoCacheTest extends AbstractJsonConverterTest {

    @Before
    public void setUp() {
        converter.configure(disableCache(Collections.emptyMap()), false);
    }

    private static Map<String, Object> disableCache(Map<String, Object> original) {
        Map<String, Object> copy = new HashMap<>(original);
        copy.put("schemas.cache.size", 0);
        return copy;
    }
}
