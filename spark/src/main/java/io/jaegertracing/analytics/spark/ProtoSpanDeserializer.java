package io.jaegertracing.analytics.spark;

import com.google.protobuf.ByteString;
import com.google.protobuf.ByteString.ByteIterator;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import io.jaegertracing.analytics.model.Span;
import io.jaegertracing.api_v2.Model;
import io.jaegertracing.api_v2.Model.KeyValue;
import io.jaegertracing.api_v2.Model.SpanRef;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class ProtoSpanDeserializer implements Deserializer<Span>, Serializable {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public Span deserialize(String topic, byte[] data) {
   try {
      Model.Span protoSpan = Model.Span.parseFrom(data);
      return fromProto(protoSpan);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException("could not deserialize to span", e);
    }
  }

  @Override
  public void close() {
  }

  private Span fromProto(Model.Span protoSpan) {
    Span span = new Span();
    span.traceID = asHexString(protoSpan.getTraceId());
    span.spanID = asHexString(protoSpan.getSpanId());
    span.operationName = protoSpan.getOperationName();
    span.serviceName = protoSpan.getProcess().getServiceName();
    span.startTime = Timestamps.toMicros(protoSpan.getStartTime());
    span.startTimeMillis = Timestamps.toMillis(protoSpan.getStartTime());
    span.duration = Durations.toMicros(protoSpan.getDuration());
    addTags(span, protoSpan.getTagsList());
    addTags(span, protoSpan.getProcess().getTagsList());
    addRefs(span, protoSpan.getReferencesList());
    /*if (protoSpan.getReferencesList().size() > 0) {
      SpanRef reference = protoSpan.getReferences(0);
      if (asHexString(reference.getTraceId()).equals(span.traceID)) {
        span.parentId = asHexString(protoSpan.getReferences(0).getSpanId());
      }
    }*/
    return span;
  }

  private static final String HEXES = "0123456789ABCDEF";

  private String asHexString(ByteString id) {
    ByteIterator iterator = id.iterator();
    StringBuilder out = new StringBuilder();
    while (iterator.hasNext()) {
      byte b = iterator.nextByte();
      out.append(HEXES.charAt((b & 0xF0) >> 4)).append(HEXES.charAt((b & 0x0F)));
    }
    return out.toString();
  }

  private void addRefs(Span span, List<SpanRef> refs) {
    for (SpanRef ref : refs) {
      Span.Reference reference = new Span.Reference();
      reference.refType = ref.getRefType().name();
      reference.spanID = ref.getSpanId().toString();
      reference.traceID = ref.getTraceId().toString();
      span.references.add(reference);
    }
  }

  private void addTags(Span span, List<KeyValue> tags) {
    for (KeyValue kv : tags) {
      if (!Model.ValueType.STRING.equals(kv.getVType())) {
        continue;
      }
      String value = kv.getVStr();
      if (value != null) {
        span.tag.put(kv.getKey(), value);
      }
    }
  }
}
