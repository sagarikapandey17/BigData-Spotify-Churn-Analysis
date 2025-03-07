package com.mycompany.highvalueusers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HighValueUserDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "High Value Users");

        // Set the Jar by class
        job.setJarByClass(HighValueUserDriver.class);

        // Set the Mapper and Reducer classes
        job.setMapperClass(HighValueUserGenreMapper.class);
        job.setReducerClass(HighValueUserGenreReducer.class);

        // Set the output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set the input and output paths (these would be HDFS paths or local paths)
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit the job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
