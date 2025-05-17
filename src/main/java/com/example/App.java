package com.example;

import java.util.List;
import java.util.Arrays;

import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.Element;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
    private static final Logger LOG = LoggerFactory.getLogger(App.class);
    private static final Double FIXED_COST = 15d;
    private static final String ABOVE_KEY = "above";
    private static final String BELOW_KEY = "below";

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<Integer> input = pipeline.apply(Create.of(10, 50, 120, 20, 200, 0));

        TupleTag<Integer> numBelow100Tag = new TupleTag<Integer>() {};
        TupleTag<Integer> numAbove100Tag = new TupleTag<Integer>() {};

        PCollectionTuple outputTuple = applyTransform(input, numBelow100Tag, numAbove100Tag);

        outputTuple.get(numBelow100Tag).apply("Nums Below 100", ParDo.of(new LogOutput<>("Number <= 100: ")));
        outputTuple.get(numAbove100Tag).apply("Nums above 100", ParDo.of(new LogOutput<>("Number > 100: ")));

        pipeline.run();
    }

    static PCollectionTuple applyTransform(
        PCollection<Integer> input, TupleTag<Integer> numBelow100Tag, TupleTag<Integer> numAbove100Tag
    ) {
        return input.apply(ParDo.of(new DoFn<Integer, Integer>() {
            @ProcessElement
            public void processElement(@Element Integer number, MultiOutputReceiver out) {
                if (number <= 100) {
                    out.get(numBelow100Tag).output(number);
                } else {
                    out.get(numAbove100Tag).output(number);
                }
            }
        }).withOutputTags(numBelow100Tag, TupleTagList.of(numAbove100Tag)));
    }

   
    static class WordsAlphabet {
        private String alphabet;
        private String fruit;
        private String country;

        public WordsAlphabet(String alphabet, String fruit, String country) {
            this.alphabet = alphabet;
            this.fruit = fruit;
            this.country = country;
        }

        @Override
        public String toString() {
            return "WordsAlphabet{" +
                    "alphabet='" + alphabet + '\'' +
                    ", fruit='" + fruit + '\'' +
                    ", country='" + country + '\'' +
                    '}';
        }
    }

static class LogOutput<T> extends DoFn<T, T> {
    private static final Logger LOG = LoggerFactory.getLogger(LogOutput.class);
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