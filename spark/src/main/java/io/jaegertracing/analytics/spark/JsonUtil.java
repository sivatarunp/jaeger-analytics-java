package io.jaegertracing.analytics.spark;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.MapType;
import io.jaegertracing.analytics.model.Span;
import io.jaegertracing.analytics.model.Trace;
import io.opentracing.tag.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Created by rashmi on 23/05/17.
 */
public final class JsonUtil {
  private static final Logger LOG = LoggerFactory.getLogger(JsonUtil.class);
  private static ObjectMapper mapper;
  private static MapType mapType;

  private JsonUtil() {
  }

  public static void init(ObjectMapper objectMapper) {
    if (mapper != null) {
      LOG.warn("init is already called. default mapper is not null");
    }
    //Configuring objectMapper
    objectMapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, true);
    objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true);
    objectMapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
    objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    objectMapper.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true);
    objectMapper.configure(DeserializationFeature.READ_ENUMS_USING_TO_STRING, true);
    objectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.LOWER_CAMEL_CASE);
    mapper = objectMapper;
    mapType = mapper.getTypeFactory().constructRawMapType(Map.class);
  }

  public static JsonNode readTree(String content) throws IOException {
    return mapper.readTree(content);
  }

  public static <T> T readValue(String content, Class<T> valueType) throws IOException {
    return mapper.readValue(content, valueType);
  }

  public static <T> T readValue(String content, TypeReference<T> valueType) throws IOException {
    return mapper.readValue(content, valueType);
  }

  public static <K, V> Map<K, V> convertJsonToHashMap(String content) throws IOException {
    return mapper.readValue(content, mapType);
  }

  public static String writeValueAsString(Object value) throws JsonProcessingException {
    return mapper.writeValueAsString(value);
  }

  public static void main(String[] args) {
    String record = "{\"meta\":{\"type\":\"span\",\"serviceName\":\"Greeting\"},\"msg\":\"{\\\"traceID\\\":\\\"3afbc2d8afb7b90490717d35935d7911\\\",\\\"spanID\\\":\\\"fa4c33df66bceb15\\\",\\\"operationName\\\":\\\"HTTP POST\\\",\\\"references\\\":[],\\\"startTime\\\":1621422120245001,\\\"startTimeMillis\\\":1621422120245,\\\"duration\\\":500890,\\\"tags\\\":[],\\\"tag\\\":{\\\"error\\\":true,\\\"http@flavor\\\":\\\"1.1\\\",\\\"http@method\\\":\\\"POST\\\",\\\"http@url\\\":\\\"http://192.168.0.124:9411/api/v2/spans\\\",\\\"internal@span@format\\\":\\\"proto\\\",\\\"net@peer@name\\\":\\\"192.168.0.124\\\",\\\"net@peer@port\\\":9411,\\\"net@transport\\\":\\\"IP.TCP\\\",\\\"otlp@instrumentation@library@name\\\":\\\"io.opentelemetry.javaagent.http-url-connection\\\",\\\"otlp@instrumentation@library@version\\\":\\\"0.17.0\\\",\\\"span@kind\\\":\\\"client\\\",\\\"status@code\\\":2,\\\"thread@id\\\":46,\\\"thread@name\\\":\\\"AsyncReporter{org.springframework.cloud.sleuth.zipkin2.sender.RestTemplateSender@79c4f23b}\\\"},\\\"logs\\\":[{\\\"timestamp\\\":1621422120745686,\\\"fields\\\":[{\\\"key\\\":\\\"message\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"exception\\\"},{\\\"key\\\":\\\"exception.message\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"connect timed out\\\"},{\\\"key\\\":\\\"exception.stacktrace\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"java.net.SocketTimeoutException: connect timed out\\\\n\\\\tat java.net.PlainSocketImpl.socketConnect(Native Method)\\\\n\\\\tat java.net.AbstractPlainSocketImpl.doConnect(AbstractPlainSocketImpl.java:350)\\\\n\\\\tat java.net.AbstractPlainSocketImpl.connectToAddress(AbstractPlainSocketImpl.java:206)\\\\n\\\\tat java.net.AbstractPlainSocketImpl.connect(AbstractPlainSocketImpl.java:188)\\\\n\\\\tat java.net.SocksSocketImpl.connect(SocksSocketImpl.java:392)\\\\n\\\\tat java.net.Socket.connect(Socket.java:589)\\\\n\\\\tat sun.net.NetworkClient.doConnect(NetworkClient.java:175)\\\\n\\\\tat sun.net.www.http.HttpClient.openServer(HttpClient.java:463)\\\\n\\\\tat sun.net.www.http.HttpClient.openServer(HttpClient.java:558)\\\\n\\\\tat sun.net.www.http.HttpClient.\\\\u003cinit\\\\u003e(HttpClient.java:242)\\\\n\\\\tat sun.net.www.http.HttpClient.New(HttpClient.java:339)\\\\n\\\\tat sun.net.www.http.HttpClient.New(HttpClient.java:357)\\\\n\\\\tat sun.net.www.protocol.http.HttpURLConnection.getNewHttpClient(HttpURLConnection.java:1220)\\\\n\\\\tat sun.net.www.protocol.http.HttpURLConnection.plainConnect0(HttpURLConnection.java:1156)\\\\n\\\\tat sun.net.www.protocol.http.HttpURLConnection.plainConnect(HttpURLConnection.java:1050)\\\\n\\\\tat sun.net.www.protocol.http.HttpURLConnection.connect(HttpURLConnection.java:984)\\\\n\\\\tat org.springframework.http.client.SimpleBufferingClientHttpRequest.executeInternal(SimpleBufferingClientHttpRequest.java:76)\\\\n\\\\tat org.springframework.http.client.AbstractBufferingClientHttpRequest.executeInternal(AbstractBufferingClientHttpRequest.java:48)\\\\n\\\\tat org.springframework.http.client.AbstractClientHttpRequest.execute(AbstractClientHttpRequest.java:53)\\\\n\\\\tat org.springframework.web.client.RestTemplate.doExecute(RestTemplate.java:742)\\\\n\\\\tat org.springframework.cloud.sleuth.zipkin2.sender.ZipkinRestTemplateWrapper.doExecute(ZipkinRestTemplateSenderConfiguration.java:228)\\\\n\\\\tat org.springframework.web.client.RestTemplate.exchange(RestTemplate.java:644)\\\\n\\\\tat org.springframework.cloud.sleuth.zipkin2.sender.RestTemplateSender.post(RestTemplateSender.java:129)\\\\n\\\\tat org.springframework.cloud.sleuth.zipkin2.sender.RestTemplateSender$HttpPostCall.doExecute(RestTemplateSender.java:142)\\\\n\\\\tat org.springframework.cloud.sleuth.zipkin2.sender.RestTemplateSender$HttpPostCall.doExecute(RestTemplateSender.java:132)\\\\n\\\\tat zipkin2.Call$Base.execute(Call.java:380)\\\\n\\\\tat zipkin2.reporter.AsyncReporter$BoundedAsyncReporter.flush(AsyncReporter.java:299)\\\\n\\\\tat zipkin2.reporter.AsyncReporter$Flusher.run(AsyncReporter.java:378)\\\\n\\\\tat java.lang.Thread.run(Thread.java:748)\\\\n\\\"},{\\\"key\\\":\\\"exception.type\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"java.net.SocketTimeoutException\\\"}]}],\\\"process\\\":{\\\"serviceName\\\":\\\"Greeting\\\",\\\"tags\\\":[],\\\"tag\\\":{\\\"host@ip\\\":\\\"127.0.0.1\\\",\\\"host@name\\\":\\\"localhost\\\",\\\"os@description\\\":\\\"Linux 4.9.119-44.140.amzn1.x86_64\\\",\\\"os@type\\\":\\\"LINUX\\\",\\\"process@command_line\\\":\\\"/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.171-7.b10.37.amzn1.x86_64/jre:bin:java -javaagent:opentelemetry-javaagent-all.jar\\\",\\\"process@executable@path\\\":\\\"/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.171-7.b10.37.amzn1.x86_64/jre:bin:java\\\",\\\"process@pid\\\":18247,\\\"process@runtime@description\\\":\\\"Oracle Corporation OpenJDK 64-Bit Server VM 25.171-b10\\\",\\\"process@runtime@name\\\":\\\"OpenJDK Runtime Environment\\\",\\\"process@runtime@version\\\":\\\"1.8.0_171-b10\\\",\\\"telemetry@auto@version\\\":\\\"0.17.0\\\",\\\"telemetry@sdk@language\\\":\\\"java\\\",\\\"telemetry@sdk@name\\\":\\\"opentelemetry\\\",\\\"telemetry@sdk@version\\\":\\\"0.17.0\\\"}}}\"}";
    //String record = "{\"meta\":{\"type\":\"span\",\"serviceName\":\"Greeting\"},\"msg\":\"{\\\"traceID\\\":\\\"bf4bc675aab2c39a49e02c3ce0fed543\\\",\\\"spanID\\\":\\\"68bd3314ca9085e3\\\",\\\"operationName\\\":\\\"/client3/moreDetails\\\",\\\"references\\\":[{\\\"refType\\\":\\\"CHILD_OF\\\",\\\"traceID\\\":\\\"bf4bc675aab2c39a49e02c3ce0fed543\\\",\\\"spanID\\\":\\\"0b237c9cbfb85ab8\\\"}],\\\"startTime\\\":1621408548900001,\\\"startTimeMillis\\\":1621408548900,\\\"duration\\\":5854,\\\"tags\\\":[],\\\"tag\\\":{\\\"http@client_ip\\\":\\\"127.0.0.1\\\",\\\"http@flavor\\\":\\\"HTTP/1.1\\\",\\\"http@method\\\":\\\"GET\\\",\\\"http@status_code\\\":200,\\\"http@url\\\":\\\"http://localhost:9082/client3/moreDetails?x=10\\\",\\\"http@user_agent\\\":\\\"Java/1.8.0_171\\\",\\\"internal@span@format\\\":\\\"proto\\\",\\\"net@peer@ip\\\":\\\"127.0.0.1\\\",\\\"net@peer@port\\\":40470,\\\"otlp@instrumentation@library@name\\\":\\\"io.opentelemetry.javaagent.tomcat\\\",\\\"otlp@instrumentation@library@version\\\":\\\"0.17.0\\\",\\\"span@kind\\\":\\\"server\\\",\\\"status@code\\\":0,\\\"thread@id\\\":30,\\\"thread@name\\\":\\\"http-nio-9082-exec-5\\\"},\\\"logs\\\":[],\\\"process\\\":{\\\"serviceName\\\":\\\"Greeting\\\",\\\"tags\\\":[],\\\"tag\\\":{\\\"host@ip\\\":\\\"127.0.0.1\\\",\\\"host@name\\\":\\\"localhost\\\",\\\"os@description\\\":\\\"Linux 4.9.119-44.140.amzn1.x86_64\\\",\\\"os@type\\\":\\\"LINUX\\\",\\\"process@command_line\\\":\\\"/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.171-7.b10.37.amzn1.x86_64/jre:bin:java -javaagent:opentelemetry-javaagent-all.jar\\\",\\\"process@executable@path\\\":\\\"/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.171-7.b10.37.amzn1.x86_64/jre:bin:java\\\",\\\"process@pid\\\":10543,\\\"process@runtime@description\\\":\\\"Oracle Corporation OpenJDK 64-Bit Server VM 25.171-b10\\\",\\\"process@runtime@name\\\":\\\"OpenJDK Runtime Environment\\\",\\\"process@runtime@version\\\":\\\"1.8.0_171-b10\\\",\\\"telemetry@auto@version\\\":\\\"0.17.0\\\",\\\"telemetry@sdk@language\\\":\\\"java\\\",\\\"telemetry@sdk@name\\\":\\\"opentelemetry\\\",\\\"telemetry@sdk@version\\\":\\\"0.17.0\\\"}}}\"}";

    System.out.println(InitialPositionInStream.valueOf("TRIM_HORIZON").name());
    try {
      System.out.println(Tags.SPAN_KIND.getKey() + ":::" + Tags.SPAN_KIND_CLIENT);
      JsonUtil.init(new ObjectMapper());
      if(JsonUtil.readTree(record) == null || !JsonUtil.readTree(record).has("msg")) {
        return;
      }
      String str = JsonUtil.readTree(record).get("msg").asText();//.replace("\\", "");
      System.out.println(str);
      System.out.println(JsonUtil.readValue(str, Span.class));
      Span span = JsonUtil.readValue(str, Span.class);

      Trace trace = new Trace();
      trace.traceId = span.traceID;
      span.serviceName = span.process.serviceName;
      trace.spans = Collections.singletonList(span);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
