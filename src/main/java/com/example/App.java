package com.example;

/**
 * Hello world!
 *
 */

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class App 
{

    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    public static void main( String[] args )
    {
        LOG.info("Running task");

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection

        pipeline.run();
        

    
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
}
