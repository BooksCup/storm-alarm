package com.bc.alarm.scheme;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author zhou
 */
public class MessageScheme implements Scheme {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    @Override
    public List<Object> deserialize(ByteBuffer byteBuffer) {
        try {
            String msg = new String(deserializeString(byteBuffer));
            return new Values(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String deserializeString(ByteBuffer byteBuffer) {
        if (byteBuffer.hasArray()) {
            int base = byteBuffer.arrayOffset();
            return new String(byteBuffer.array(), base + byteBuffer.position(), byteBuffer.remaining());
        } else {
            return new String(Utils.toByteArray(byteBuffer), StandardCharsets.UTF_8);
        }
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("msg");
    }
}