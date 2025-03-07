package com.mycompany.userengagement_job1;

import com.mycompany.userengagement_job1.NormalizationMapper;
import com.mycompany.userengagement_job1.NormalizationReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InteractionAnalysisDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: InteractionAnalysisDriver <input path> <temp output path> <final output path>");
            System.exit(-1);
        }

        // Job 1: Interaction Aggregation
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Interaction Aggregation");
        job1.setJarByClass(InteractionAnalysisDriver.class);
        job1.setMapperClass(InteractionAggregatorMapper.class);
        job1.setReducerClass(InteractionAggregatorReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // Job 2: Normalize Interactions
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Normalize Interactions");
        job2.setJarByClass(InteractionAnalysisDriver.class);
        job2.setMapperClass(NormalizationMapper.class);
        job2.setReducerClass(NormalizationReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }
    }
}
