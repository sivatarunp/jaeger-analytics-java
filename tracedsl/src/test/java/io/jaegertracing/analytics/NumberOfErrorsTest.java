package io.jaegertracing.analytics;

import io.jaegertracing.analytics.gremlin.GraphCreator;
import io.jaegertracing.analytics.model.Span;
import io.jaegertracing.analytics.model.Trace;
import io.opentracing.tag.Tags;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Set;

/**
 * @author Pavol Loffay
 */
public class NumberOfErrorsTest {

  @Test
  public void testEmpty() {
    Assert.assertEquals(0, NumberOfErrors.calculate(GraphCreator.create(new Trace())).size());
  }

  @Test
  public void testCalculate() {
    Span root = Util.newTrace("root", "root");
    Span.Tag tag = new Span.Tag();
    tag.key = Tags.ERROR.getKey();
    tag.value = "true";
    tag.type = "string";
    root.tags.add(tag);

    Span child = Util.newChild("root", "child", root);
    tag = new Span.Tag();
    tag.key = Tags.ERROR.getKey();
    tag.value = "false";
    tag.type = "string";
    child.tags.add(tag);

    Span childChild = Util.newChild("root", "child", child);
    tag = new Span.Tag();
    tag.key = Tags.ERROR.getKey();
    tag.value = "true";
    tag.type = "string";
    child.tags.add(tag);

    Trace trace = new Trace();
    trace.spans.addAll(Arrays.asList(root, child, childChild));
    Set<Span> errorSpans = NumberOfErrors.calculate(GraphCreator.create(trace));
    Assert.assertEquals(2, errorSpans.size());
  }
}
