package net.radai.hermann.util;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * simple utility code for serializing and deserializing 
 * kafka wire format using the vanilla code 
 */
public class KafkaWireFormat {
    private final static Map<Class<? extends AbstractRequest>, RequestDesc> API_KEYS_MAP;

    static {
        API_KEYS_MAP = buildKeysMap();
    }
    
    public static ApiKeys keyFor(AbstractRequest req) {
        return descFor(req).apiKey;
    }
    
    public static byte[] serialize(AbstractRequest req) {
        return serialize(req, -1, "bob", 666);
    }
    
    public static byte[] serialize(AbstractRequest req, int asVersion, String clientId, int correlation) {
        if (req == null) {
            throw new IllegalArgumentException("request cannot be null");
        }
        if (asVersion > Short.MAX_VALUE || asVersion < Short.MIN_VALUE) {
            throw new IllegalArgumentException("asVersion (" + asVersion + ") must be a valid short");
        }
        short version = (short) asVersion;
        RequestDesc desc = descFor(req);
        ApiKeys apiKey = desc.apiKey;
        Method toStructMethod = desc.toStructMethod;
        if (version < 0) {
            version = apiKey.latestVersion();
        }
        if (!apiKey.isVersionSupported(version)) {
            throw new IllegalArgumentException("no such thing as " + apiKey + " v" + version);
        }
        Struct requestStrust;
        try {
            requestStrust = (Struct) toStructMethod.invoke(req);
        } catch (Exception e) {
            throw new IllegalArgumentException("unable to invoke " + toStructMethod, e);
        }
        RequestHeader header = new RequestHeader(apiKey, version, clientId, correlation);
        Struct headerStruct = header.toStruct();
        int payloadSize = requestStrust.sizeOf() + headerStruct.sizeOf();
        int withPrefix = payloadSize + 4;
        byte[] bytes = new byte[withPrefix];
        ByteBuffer wrapper = ByteBuffer.wrap(bytes);
        wrapper.putInt(payloadSize);
        headerStruct.writeTo(wrapper);
        requestStrust.writeTo(wrapper);
        return bytes;
    }
    
    public static ResponseHeader deserializeHeader(byte[] bytes, int fromPosition) {
        ByteBuffer wrapper = ByteBuffer.wrap(bytes, fromPosition, bytes.length - fromPosition);
        ResponseHeader responseHeader = ResponseHeader.parse(wrapper);
        return responseHeader;
    }
    
    public static AbstractResponse deserialize(byte[] bytes, ApiKeys request, short requestVersion, int fromPosition) {
        ByteBuffer wrapper = ByteBuffer.wrap(bytes, fromPosition, bytes.length - fromPosition);
        ResponseHeader responseHeader = ResponseHeader.parse(wrapper); 
        Struct responseStruct = request.parseResponse(requestVersion, wrapper);
        AbstractResponse response = AbstractResponse.parseResponse(request, responseStruct);
        return response;
    }

    private static RequestDesc descFor(AbstractRequest req) {
        Class<? extends AbstractRequest> clazz = req.getClass();
        RequestDesc desc = API_KEYS_MAP.get(clazz);
        if (desc == null) {
            throw new IllegalStateException("unknown request class " + clazz.getCanonicalName());
        }
        return desc;
    }

    /**
     * this method exists because one does not simply get the type of a kafka AbstractRequest
     * @return metadata about all the requests supported by the runtime version of kafka
     */
    private static Map<Class<? extends AbstractRequest>, RequestDesc> buildKeysMap() {
        ApiKeys[] apiKeys = ApiKeys.values();
        Map<Class<? extends AbstractRequest>, RequestDesc> results = new HashMap<>(apiKeys.length);
        for (ApiKeys apiKey : apiKeys) {
            String enumName = apiKey.name();
            //this is an attempt to not be bound to a particular version (and hence a particular set
            //of supported API keys). kafka _mostly_ has a consistent naming scheme for requests
            //but since this is kafka, only mostly.
            if ("LIST_OFFSETS".equals(enumName)) {
                enumName = "LIST_OFFSET";
            } else if ("OFFSET_FOR_LEADER_EPOCH".equals(enumName)) {
                enumName = "OFFSETS_FOR_LEADER_EPOCH";
            }
            String[] words = enumName.split("_");
            StringBuilder sb = new StringBuilder();
            sb.append("org.apache.kafka.common.requests.");
            for (String word : words) {
                sb.append(word.substring(0, 1).toUpperCase(Locale.ROOT));
                sb.append(word.substring(1).toLowerCase(Locale.ROOT));
            }
            sb.append("Request");
            String fqcn = sb.toString();
            Class<? extends AbstractRequest> clazz;
            try {
                //noinspection unchecked
                clazz = (Class<? extends AbstractRequest>) Class.forName(fqcn);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException("unable to locate request class " + fqcn + " for " + apiKey);
            }
            Method toStructMethod;
            try {
                toStructMethod = clazz.getDeclaredMethod("toStruct");
                toStructMethod.setAccessible(true);
            } catch (NoSuchMethodException e) {
                throw new IllegalStateException("unable to locate " + fqcn + ".toStruct()");
            }
            results.put(clazz, new RequestDesc(apiKey, toStructMethod));
        }
        return Collections.unmodifiableMap(results);
    }
    
    private static class RequestDesc {
        private final ApiKeys apiKey;
        private final Method toStructMethod;

        public RequestDesc(ApiKeys apiKey, Method toStructMethod) {
            this.apiKey = apiKey;
            this.toStructMethod = toStructMethod;
        }
    }
}
