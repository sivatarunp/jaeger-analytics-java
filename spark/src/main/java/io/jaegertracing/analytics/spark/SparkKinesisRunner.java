package io.jaegertracing.analytics.spark;

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.jaegertracing.analytics.DirectDependencies;
import io.jaegertracing.analytics.ModelRunner;
import io.jaegertracing.analytics.NetworkLatency;
import io.jaegertracing.analytics.NumberOfErrors;
import io.jaegertracing.analytics.ServiceDepth;
import io.jaegertracing.analytics.ServiceHeight;
import io.jaegertracing.analytics.TraceHeight;
import io.jaegertracing.analytics.gremlin.GraphCreator;
import io.jaegertracing.analytics.model.Span;
import io.jaegertracing.analytics.model.Trace;
import io.jaegertracing.analytics.tracequality.HasClientServerSpans;
import io.jaegertracing.analytics.tracequality.UniqueSpanId;
import io.prometheus.client.exporter.HTTPServer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kinesis.KinesisInitialPositions;
import org.apache.spark.streaming.kinesis.KinesisInputDStream;
import org.apache.tinkerpop.gremlin.structure.Graph;
import scala.Tuple2;
import scala.reflect.ClassTag$;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPInputStream;

/**
 * @author Pavol Loffay
 */
public class SparkKinesisRunner {

    /**
     * System Properties that can be passed
     * SPARK_MASTER
     * SPARK_MEMORY
     * SPARK_STREAMING_BATCH_DURATION
     * SPARK_STREAMING_WINDOW_DURATION - should be multiple of batch duration
     * AWS_REGION
     * KINESIS_STREAM
     * KINESIS_STREAM_POSITION
     * PROMETHEUS_HOST
     * PROMETHEUS_PORT
     * All the above have default values
     * @param args
     * @throws InterruptedException
     * @throws IOException
     */
    public static void main(String[] args) throws InterruptedException, IOException {
        HTTPServer server = new HTTPServer(getPropOrEnv("PROMETHEUS_HOST", "localhost"), Integer.parseInt(getPropOrEnv("PROMETHEUS_PORT", "9111")));

        JsonUtil.init(new ObjectMapper());

        SparkConf sparkConf = new SparkConf()
                .setAppName("Trace DSL")
                .setMaster(getPropOrEnv("SPARK_MASTER", "local[*]"))
                .set("spark.testing.memory", getPropOrEnv("SPARK_MEMORY", "471859200"));

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        long batchInterval = Integer.parseInt(getPropOrEnv("SPARK_STREAMING_BATCH_DURATION", "10000"));
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(batchInterval));

        System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true");
        System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");

        //TODO: Check for partition based parallelization with multiple kinesisStreams
        // int partitions = Integer.parseInt(getPropOrEnv("STREAM_PARTITIONS", "1"));

        String region = Regions.getCurrentRegion()!=null ? Regions.getCurrentRegion().getName()
                : getPropOrEnv("AWS_REGION", Regions.US_EAST_1.getName());

        InitialPositionInStream initialPosition;
        try {
            initialPosition = InitialPositionInStream
                    .valueOf(getPropOrEnv("KINESIS_STREAM_POSITION", "TRIM_HORIZON"));
        } catch (IllegalArgumentException e) {
            initialPosition = InitialPositionInStream.valueOf("TRIM_HORIZON");
        }

        KinesisInputDStream<byte[]> kinesisStream = KinesisInputDStream.builder()
                .streamingContext(ssc)
                .regionName(region)
                .streamName(getPropOrEnv("KINESIS_STREAM", "common_haystack_traces"))
                .initialPosition(KinesisInitialPositions.fromKinesisInitialPosition(initialPosition))
                .checkpointAppName("trace-analytics")
                .checkpointInterval(Duration.apply(60 * 1000))
                .build();

        JavaDStream<byte[]> dStream = JavaDStream.fromDStream(kinesisStream, ClassTag$.MODULE$.apply(byte[].class));

        long windowInterval = Integer.parseInt(getPropOrEnv("SPARK_STREAMING_WINDOW_DURATION", "60000"));
        JavaDStream<Span> spanStream = dStream.window(Duration.apply(windowInterval)).flatMap((FlatMapFunction<byte[], Span>) kinesisRecord -> {
            String payload = new String(decompress(kinesisRecord), StandardCharsets.UTF_8);
            String[] records = payload.split(System.lineSeparator());
            List<Span> spanList = new LinkedList<>();
            for (String record: records) {
                try {
                    if(JsonUtil.readTree(record) == null || !JsonUtil.readTree(record).has("msg")) {
                        continue;
                    }
                    String spanStr = JsonUtil.readTree(record).get("msg").asText();
                    Span span = JsonUtil.readValue(spanStr, Span.class);
                    if(span.process != null && span.process.serviceName != null) {
                        span.serviceName = span.process.serviceName;
                        spanList.add(span);
                    }
                } catch (Exception e) {
                    System.out.println("Exception for record : "+record);
                    e.printStackTrace();
                }
            }
            return spanList.iterator();
        });

        JavaPairDStream<String, Span> traceIdSpanTuple = spanStream.mapToPair(record -> new Tuple2<>(record.traceID, record));
        JavaDStream<Trace> tracesStream = traceIdSpanTuple.groupByKey().map(traceIdSpans -> {
            System.out.printf("TraceID: %s\n", traceIdSpans._1);
            Iterable<Span> spans = traceIdSpans._2();
            Trace trace = new Trace();
            trace.traceId = traceIdSpans._1();
            trace.spans = StreamSupport.stream(spans.spliterator(), false)
                    .collect(Collectors.toList());
            return trace;
        });

        List<ModelRunner> modelRunner = Arrays.asList(
                new TraceHeight(),
                new ServiceDepth(),
                new ServiceHeight(),
                new NetworkLatency(),
                new NumberOfErrors(),
                new DirectDependencies(),
                // trace quality
                new HasClientServerSpans(),
                new UniqueSpanId());

        tracesStream.foreachRDD((traceRDD, time) -> traceRDD.foreach(trace -> {
            Graph graph = GraphCreator.create(trace);

            for (ModelRunner model : modelRunner) {
                model.runWithMetrics(graph);
            }
        }));

        ssc.start();
        ssc.awaitTermination();
    }

    private static String getPropOrEnv(String key, String defaultValue) {
        String value = System.getProperty(key, System.getenv(key));
        return value != null ? value : defaultValue;
    }

    public static byte[] decompress(byte[] payload) throws IOException {
        final int gzip_header = (payload[0] & 0xff) | ((payload[1] << 8) & 0xff00);
        if (gzip_header != GZIPInputStream.GZIP_MAGIC) {
            return payload;
        }

        byte[] buf = new byte[1024];
        final ByteArrayInputStream bis = new ByteArrayInputStream(payload);
        final GZIPInputStream gzip = new GZIPInputStream(bis);
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        int len;
        while ((len = gzip.read(buf)) > 0) {
            bos.write(buf, 0, len);
        }
        gzip.close();
        bis.close();
        byte[] decompressed = bos.toByteArray();
        bos.close();

        return decompressed;
    }
}
