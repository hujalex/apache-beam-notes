package com.example;

import java.util.List;
import java.util.Arrays;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        LOG.info("Running task");

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<KV<Integer,Integer>> input = pipeline.apply(
            Create.of(KV.of(1,11),
            KV.of(1,36),
            KV.of(2, 91),
            KV.of(3, 33),
            KV.of(3, 11),
            KV.of(4, 33))
        );

        PCollection<Long> output = applyTransform(input);

        output.apply("Log", ParDo.of(new LogOutput<>("Input has elements")));




        pipeline.run();

        // Option 2: Inline anonymous DoFn
        // input.apply(ParDo.of(new DoFn<String, String>() {
        //     @ProcessElement
        //     public void processElement(ProcessContext c) throws Exception {
        //         LOG.info("Inline processing word: {}", c.element());
        //         c.output(c.element());
        //     }
        // }));

    }

    static PCollection<Long> applyTransform(PCollection<KV<Integer, Integer>> input) {
        return input.apply(Count.globally());
    }

    public static class LogStrings extends DoFn<String, String> {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOG.info("Processing word: {}", c.element());
            c.output(c.element());
        }
    }

    public static class LogIntegers extends DoFn<Integer, Integer> {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOG.info("Processing number: {}", c.element());
            c.output(c.element());
        }
    }

    public static class LogLongs extends DoFn<Long, Long> {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOG.info("Processing number: {}", c.element());
            c.output(c.element());
        }
    }

    static class LogOutput<T> extends DoFn<T, T> {
        private String prefix;

        LogOutput() {
            this.prefix = "Processing element";
        }

        LogOutput(String prefix) {
            this.prefix = prefix;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOG.info(prefix + ": {}", c.element());
        }
    }
}