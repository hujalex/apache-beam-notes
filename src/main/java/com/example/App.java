package com.example;

import java.util.List;
import java.util.Arrays;

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
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
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

        PCollection<Integer> input = pipeline.apply(Create.of(10, 20, 30, 40, 50));
        PCollection<Integer> output = applyTransform(input);


        output.apply("LOG", ParDo.of(new LogOutput<Integer>()));

        pipeline.run();
    }

    static PCollection<Integer> applyTransform(PCollection<Integer> input) {
        return input.apply(
            MapElements.into(TypeDescriptors.integers())
                .via(number -> number * 10)
        );
    }

    static PCollection<Double> getSum(PCollection<Double> input, String name) {

        return input.apply(Sum.doublesGlobally());
    }

    static PCollection<Double> getAboveCost(PCollection<Double> input) {

        return input.apply(Filter.greaterThanEq(15.0));
    }

    static PCollection<Double> getBelowCost(PCollection<Double> input) {

        return input.apply(Filter.lessThan(15.0));
    }

    // Define the type PCollection<?>
    static PCollection<KV<String, Double>> setKeyForCost(PCollection<Double> input, String key) {
        return input.apply("Set Key " + key, ParDo.of(new DoFn<Double, KV<String, Double>>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                c.output(KV.of(key, c.element()));
            }
        }));
    }

    static class ExtractTaxiRideCostFn extends DoFn<String, Double> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] items = c.element().split(",");
            Double totalAmount = tryParseTaxiRideCost(items);
            c.output(totalAmount);
        }
    }

    private static String tryParseString(String[] inputItems, int index) {
        return inputItems.length > index ? inputItems[index] : null;
    }

    private static Double tryParseTaxiRideCost(String[] inputItems) {
        try {
            return Double.parseDouble(tryParseString(inputItems, 16));
        } catch (NumberFormatException | NullPointerException e) {
            return 0.0;
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