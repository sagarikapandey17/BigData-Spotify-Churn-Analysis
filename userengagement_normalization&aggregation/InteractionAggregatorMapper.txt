package com.mycompany.userengagement_job1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class InteractionAggregatorMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final Text compositeKey = new Text();
    private final IntWritable one = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length < 12) {
            return; // Skip invalid rows
        }

        String subscription = fields[5].trim();
        String action = fields[10].trim();
        String genre = fields[9].trim();
        String device = fields[11].trim();

        // Construct composite key
        compositeKey.set(subscription + "\t" + action + "\t" + genre + "\t" + device);

        // Emit composite key with count 1
        context.write(compositeKey, one);
    }
}
