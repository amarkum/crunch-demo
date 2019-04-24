package com.codefolx.test;

import org.apache.crunch.*;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.To;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;

public class CrunchDemo {
    public static void main(String[] args) {

        // Input and Output path
        String inputPath = args[0];
        String outputPath = args[1];

        //Create a new Hadoop Job Configuration
        Configuration configuration = new Configuration();

        //Create a Pipeline
        Pipeline pipeline = getPipeline(configuration);

        //Read PCollection of String from Text file.
        PCollection person_data = pipeline.readTextFile(inputPath);

        //Create a PCollection of Person record.
        PCollection <Person> person_record = person_data.parallelDo(DoFn_CreatePojo(), Avros.records(Person.class));

        // Materialize PCollection
        for (Person person : person_record.materialize()){
            System.out.println(person.getName()+" lives in "+person.getCity()+" city ");
        }

        //Write collection to avro file.
        person_record.write(To.avroFile(outputPath));

        PipelineResult result = pipeline.done();
        System.exit(result.succeeded() ? 0 : 1);
    }

    static DoFn < String, Person > DoFn_CreatePojo() {
        return new DoFn < String, Person > () {
            @Override
            public void process(String input, Emitter < Person > emitter) {
                String input_parts[] = input.split(",");
                String name = input_parts[0];
                String roll = input_parts[1];
                String city = input_parts[2];
                emitter.emit(new Person(name, roll, city));
            }
        };
    }

    public static Pipeline getPipeline(final Configuration conf) {
        conf.setBoolean("mapred.output.compress", false);
        return new MRPipeline(Person.class, conf);
    }
}
