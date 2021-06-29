package io.jaegertracing.analytics.gremlin;

import io.jaegertracing.analytics.model.Span;
import io.jaegertracing.analytics.model.Trace;
import io.opentracing.References;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.util.LinkedHashMap;
import java.util.Map;

public class GraphCreator {

  private GraphCreator() {}
/*  private static final Counter ORPHAN_SPAN_COUNTER = Counter.build()
          .name("orphan_span_total")
          .help("Number of orphan spans within single trace")
          .labelNames("service", "operation")
          .create()
          .register();

  private static final Counter ORPHAN_SPAN_TRACE_COUNTER = Counter.build()
          .name("orphan_span_trace_total")
          .help("Number of traces with orphan spans")
          .create()
          .register();

  private static final Counter TRACE_COUNTER = Counter.build()
          .name("root_span_trace_total")
          .help("Number of traces with root spans")
          .create()
          .register();

  private static final Histogram TRACE_SPAN_COUNTER = Histogram.build()
          .name("root_span_trace_span_total")
          .help("Number of spans in trace with root span")
          .create()
          .register();
*/
  public static Graph create(Trace trace) {
    TinkerGraph graph = TinkerGraph.open();

    // create vertices
    Map<String, Vertex> vertexMap = new LinkedHashMap<>();
    for (Span span: trace.spans) {
      Vertex vertex = graph.addVertex(Keys.SPAN_TYPE);
      vertexMap.put(span.spanID, vertex);

      vertex.property(Keys.SPAN, span);
      vertex.property(Keys.TRACE_ID, span.traceID);
      vertex.property(Keys.SPAN_ID, span.spanID);
      if (!span.references.isEmpty()) {
        vertex.property(Keys.PARENT_ID, span.references.get(0).spanID);
      }
      vertex.property(Keys.START_TIME, span.startTime);
      vertex.property(Keys.DURATION, span.duration);
      vertex.property(Keys.SERVICE_NAME, span.serviceName);
      vertex.property(Keys.OPERATION_NAME, span.operationName);
      span.tag.entrySet().forEach(stringStringEntry -> {
        vertex.property(stringStringEntry.getKey().replace("@", "."), stringStringEntry.getValue());
      });
      span.tags.forEach(tag -> {
        vertex.property(tag.key.replace("@","."), tag.value);
      });
    }

    boolean hasOrphanSpan = false;
    for (Span span: trace.spans) {
      Vertex vertex = vertexMap.get(span.spanID);
      if (!span.references.isEmpty()) {
        //TODO: To handle case if span has multiple references.
        Vertex parent = vertexMap.get(span.references.get(0).spanID);
        if (parent != null) {
          parent.addEdge(References.CHILD_OF, vertex);
        } else {
          //ORPHAN_SPAN_COUNTER.labels(span.serviceName, span.operationName).inc();
          hasOrphanSpan = true;
        }
      } else {
        //TRACE_COUNTER.inc();
        //TRACE_SPAN_COUNTER.observe(trace.spans.size());
      }
    }
    if(hasOrphanSpan) {
      //ORPHAN_SPAN_TRACE_COUNTER.inc();
    }

    return graph;
  }

  public static Span toSpan(Vertex vertex) {
    return (Span) vertex.property(Keys.SPAN).value();
  }
}
