package com.mycompany.churnprediction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ChurnAnalysisDriver {

    public static void main(String[] args) throws Exception {
        // Configuration setup for the job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Churn Analysis");

        // Setting the Jar class for the job to ensure the correct class is loaded
        job.setJarByClass(ChurnAnalysisDriver.class);

        // Setting the Mapper and Reducer classes
        job.setMapperClass(ChurnAnalysisJobMapper.class);
        job.setReducerClass(ChurnAnalysisJobReducer.class);

        // Setting the input and output key-value types for the Mapper
        job.setMapOutputKeyClass(UserDeviceKey.class);   // Mapper's output key type 
        job.setMapOutputValueClass(Text.class);           // Mapper's output value type 

        // Setting the input and output key-value types for the Reducer
        job.setOutputKeyClass(UserDeviceKey.class);       // Reducer's input key type
        job.setOutputValueClass(Text.class);              // Reducer's input value type

        // Setting input and output paths
        if (args.length != 2) {
            System.err.println("Usage: ChurnAnalysisDriver <input path> <output path>");
            System.exit(-1);
        }

        // Adding input path for the job
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // Set the output path for the job
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Execute the job and return the exit status
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
