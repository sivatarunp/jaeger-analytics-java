package io.jaegertracing.analytics.model;

import org.apache.tinkerpop.shaded.jackson.annotation.JsonProperty;
import org.apache.tinkerpop.shaded.jackson.databind.annotation.JsonSerialize;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.MapSerializer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Span implements Serializable {
    public String traceID;
    public String spanID;
    // TODO add references array
    //public String parent_Id;
    public String serviceName;
    public String operationName;
    public long startTimeMillis;
    public long startTime;
    public long duration;
    @JsonProperty("tag")
    @JsonSerialize(using = MapSerializer.class)
//    @JsonDeserialize(keyUsing = String.class)
    public Map<String, String> tag = new HashMap<>();
    public List<Tag> tags = new ArrayList<>();
    public List<Log> logs = new ArrayList<>();
    public List<Reference> references = new ArrayList<>();
    public Process process;

    public static class Tag implements Serializable {
        public String key;
        public String value;
        public String type;

        @Override
        public String toString() {
            return "Tag{" +
                    "key='" + key + '\'' +
                    ", value='" + value + '\'' +
                    ", type='" + type + '\'' +
                    '}';
        }
    }

    public static class Log implements Serializable {
        public long timestamp;
        public List<Map<String, String>> fields = new ArrayList<>();
    }

    public static class Reference implements Serializable {
        public String traceID;
        public String spanID;
        public String refType;

        @Override
        public String toString() {
            return "Reference{" +
                    "traceID='" + traceID + '\'' +
                    ", spanID='" + spanID + '\'' +
                    ", refType='" + refType + '\'' +
                    '}';
        }
    }

    public static class Process implements Serializable {
        public String serviceName;
        public Map<String, String> tag = new HashMap<>();
        public List<Tag> tags = new ArrayList<>();

        @Override
        public String toString() {
            return "Process{" +
                    "serviceName='" + serviceName + '\'' +
                    ", tag=" + tag +
                    ", tags=" + tags +
                    '}';
        }
    }

    @Override
    public String toString() {
        return "Span{" +
                "traceID='" + traceID + '\'' +
                ", spanID='" + spanID + '\'' +
                ", serviceName='" + serviceName + '\'' +
                ", operationName='" + operationName + '\'' +
                ", startTimeMillis=" + startTimeMillis +
                ", startTime=" + startTime +
                ", duration=" + duration +
                ", tag=" + tag +
                ", tags=" + tags +
                ", logs=" + logs +
                ", references=" + references +
                ", process=" + process +
                '}';
    }
}
