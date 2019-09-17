package com.openlattice.shuttle.payload;

import java.util.Map;
import java.util.stream.Stream;

public interface Payload {
    Stream<Map<String, Object>> getPayload();
}
