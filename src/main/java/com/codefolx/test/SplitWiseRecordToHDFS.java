package com.codefolx.test;

import org.apache.crunch.*;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.To;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;

import java.util.Arrays;

public class SplitWiseRecordToHDFS {
    public static void main(String[] args) {

        // Input and Output path
        String inputPath = args[0];
        String outputPath = args[1];

        //Create a new Hadoop Job Configuration
        Configuration configuration = new Configuration();

        //Create a Pipeline
        Pipeline pipeline = getPipeline(configuration);

        //Read PCollection of String from Text file.
        PCollection splitWiseEntryCommaSeparated = pipeline.readTextFile(inputPath);

        //Create a PCollection of Person record.
        PCollection personRecord = splitWiseEntryCommaSeparated.parallelDo(DoFnCreateSplitWiseRecord(), Avros.records(SplitWiseRecord.class));

        //Write collection to avro file.
        personRecord.write(To.avroFile(outputPath), Target.WriteMode.OVERWRITE);

        PipelineResult result = pipeline.done();
        System.exit(result.succeeded() ? 0 : 1);
    }

    public static DoFn<String, SplitWiseRecord> DoFnCreateSplitWiseRecord() {
        return new DoFn<String, SplitWiseRecord>() {
            @Override
            public void process(String input, Emitter<SplitWiseRecord> emitter) {
                String inputParts[] = input.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                    String date = inputParts[0];
                    String description = inputParts[1];
                    String category = inputParts[2];
                    double cost = Double.parseDouble(inputParts[3]);
                    String currency = inputParts[4];
                    double roomMate1 = Double.parseDouble(inputParts[5]);
                    double roomMate2 = Double.parseDouble(inputParts[6]);
                    double roomMate3 = Double.parseDouble(inputParts[7]);
                    emitter.emit(new SplitWiseRecord(date, description, category, cost , currency, roomMate1,roomMate2,roomMate3));
            }
        };
    }

    public static Pipeline getPipeline(final Configuration conf) {
        conf.setBoolean("mapred.output.compress", false);
        return new MRPipeline(Person.class, conf);
    }
}
