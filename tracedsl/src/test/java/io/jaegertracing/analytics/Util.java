package io.jaegertracing.analytics;

import io.jaegertracing.analytics.model.Span;
import io.jaegertracing.api_v2.Model;

import java.util.UUID;

/**
 * @author Pavol Loffay
 */
public class Util {

  private Util() {}

  public static Span newTrace(String serviceName, String operationName) {
    Span span = new Span();
    span.serviceName = serviceName;
    span.operationName = operationName;
    span.spanID = UUID.randomUUID().toString();
    span.traceID = UUID.randomUUID().toString();
    return span;
  }

  public static Span newChild(String serviceName, String operationName, Span parent) {
    Span span = new Span();
    span.serviceName = serviceName;
    span.operationName = operationName;
    span.spanID = UUID.randomUUID().toString();
    span.traceID = parent.traceID;
    Span.Reference reference = new Span.Reference();
    reference.spanID = parent.spanID;
    reference.traceID = parent.traceID;
    reference.refType = Model.SpanRefType.CHILD_OF.name();
    span.references.add(reference);
    //span.parentId = parent.spanID;
    return span;
  }
}
