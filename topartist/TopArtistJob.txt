package com.mycompany.topartist;


import com.mycompany.topartist.TopArtistMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopArtistJob {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top Artists per Year");
        job.setJarByClass(TopArtistJob.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(TopArtistMapper.class);
        job.setReducerClass(TopArtistReducer.class);

        // Set output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0])); // Input path
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output path

        // Optionally, add sorting and filtering based on ranking score in the output

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
