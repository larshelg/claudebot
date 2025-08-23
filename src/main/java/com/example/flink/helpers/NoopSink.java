package com.example.flink.helpers;

import org.apache.flink.api.connector.sink2.InitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

public  class NoopSink<T> implements Sink<T> {
    @Override
    public SinkWriter<T> createWriter(InitContext context) {
        return new SinkWriter<T>() {
            @Override
            public void write(T element, Context context) {
            }

            @Override
            public void flush(boolean endOfInput) {
            }

            @Override
            public void close() {
            }
        };
    }
}
