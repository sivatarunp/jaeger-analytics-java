package io.jaegertracing.analytics.model;

import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import io.jaegertracing.api_v2.Model;
import io.jaegertracing.api_v2.Model.KeyValue;
import io.jaegertracing.api_v2.Model.Log;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author Pavol Loffay
 */
public class Converter {
  private Converter() {}

  public static Trace toModel(List<Model.Span> spanList) {
    // gRPC query service returns list of spans not the trace object
    Trace trace = new Trace();
    for (Model.Span protoSpan: spanList) {
      if (trace.traceId == null || trace.traceId.isEmpty()) {
        trace.traceId = toStringId(protoSpan.getTraceId());
      }
      trace.spans.add(toModel(protoSpan));
    }
    return trace;
  }

  public static Collection<Trace> toModelTraces(List<Model.Span> spanList) {
    Map<String, Trace> traces = new LinkedHashMap<>();
    for (Model.Span protoSpan: spanList) {
      Span span = toModel(protoSpan);
      Trace trace = traces.get(span.traceID);
      if (trace == null) {
        trace = new Trace();
        trace.traceId = span.traceID;
        traces.put(span.traceID, trace);
      }
      trace.spans.add(span);
    }
    return traces.values();
  }

  public static Span toModel(Model.Span protoSpan) {
    Span span = new Span();
    span.spanID = toStringId(protoSpan.getSpanId());
    span.traceID = toStringId(protoSpan.getTraceId());
    if (protoSpan.getReferencesList().size() > 0) {
      Model.SpanRef parent = protoSpan.getReferencesList().get(0);
      Span.Reference reference = new Span.Reference();
      reference.spanID = parent.getSpanId().toString();
      reference.traceID = parent.getTraceId().toString();
      reference.refType = Model.SpanRefType.CHILD_OF.name();
      span.references.add(reference);
    }

    span.serviceName = protoSpan.getProcess().getServiceName();
    span.operationName = protoSpan.getOperationName();
    span.startTime = timestampToMicros(protoSpan.getStartTime());
    span.duration = durationToMicros(protoSpan.getDuration());

    span.tag = toMap(protoSpan.getTagsList());
    span.logs = new ArrayList<>();
    for (Log protoLog: protoSpan.getLogsList()) {
      Span.Log log = new Span.Log();
      log.timestamp = timestampToMicros(protoLog.getTimestamp());
      log.fields = new ArrayList<>();
      for (KeyValue keyValue : protoLog.getFieldsList()) {
        Map<String, String> keyValueMap = new HashMap<>();
        keyValueMap.put("key", keyValue.getKey());
        keyValueMap.put("value", keyValue.getVStr());
        keyValueMap.put("type", keyValue.getVType().name());
        log.fields.add(keyValueMap);
      }
      protoLog.getFieldsList();
      span.logs.add(log);
    }

    return span;
  }

  private static long durationToMicros(Duration duration) {
    long nanos = TimeUnit.SECONDS.toNanos(duration.getSeconds());
    nanos += duration.getNanos();
    return TimeUnit.NANOSECONDS.toMicros(nanos);
  }

  private static long timestampToMicros(Timestamp timestamp) {
    long nanos = TimeUnit.SECONDS.toNanos(timestamp.getSeconds());
    nanos += timestamp.getNanos();
    return TimeUnit.NANOSECONDS.toMicros(nanos);
  }

  private static Map<String, String> toMap(List<KeyValue> tags) {
    Map<String, String> tagMap = new LinkedHashMap<>();
    for (KeyValue keyValue: tags) {
      tagMap.put(keyValue.getKey(), toStringValue(keyValue));
    }
    return tagMap;
  }

  private static String toStringValue(KeyValue keyValue) {
    switch (keyValue.getVType()) {
      case STRING:
        return keyValue.getVStr();
      case BOOL:
        return Boolean.toString(keyValue.getVBool());
      case INT64:
        return Long.toString(keyValue.getVInt64());
      case FLOAT64:
        return Double.toString(keyValue.getVFloat64());
      case BINARY:
        return keyValue.getVBinary().toStringUtf8();
      case UNRECOGNIZED:
      default:
        return "unrecognized";
    }
  }

  public static String toStringId(ByteString id) {
    return new BigInteger(1, id.toByteArray()).toString(16);
  }

  public static ByteString toProtoId(String id) {
    return ByteString.copyFrom(new BigInteger(id, 16).toByteArray());
  }
}
