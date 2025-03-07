package com.mycompany.reducejoins;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ReduceSideJoinDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: ReduceSideJoinDriver <songs_input_path> <interactions_input_path> <output_path>");
            System.exit(-1);
        }

        // Configuration and Job setup
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Reduce-Side Join for Songs and Interactions");
        job.setJarByClass(ReduceSideJoinDriver.class);

        // Mapper classes for each dataset
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, SongJoinMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, InteractionJoinMapper.class);

        // Reducer class
        job.setReducerClass(JoinReducer.class);

        // Set output key and value classes
        job.setOutputKeyClass(Text.class); // Song ID
        job.setOutputValueClass(Text.class); // Joined result as text

        // Intermediate (mapper) output key and value classes
        job.setMapOutputKeyClass(Text.class); 
        job.setMapOutputValueClass(CustomWritable.class);

        // Set input and output formats
        job.setOutputFormatClass(TextOutputFormat.class); // Ensures text-based output

        // Output path
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
