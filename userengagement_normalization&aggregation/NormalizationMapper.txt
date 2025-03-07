package com.mycompany.userengagement_job1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NormalizationMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final Text subscriptionKey = new Text();
    private final IntWritable interactionCount = new IntWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        if (fields.length != 5) { // subscription, action, genre, device, count
            return;
        }

        String subscription = fields[0].trim();
        int count = Integer.parseInt(fields[4].trim());

        subscriptionKey.set(subscription);
        interactionCount.set(count);

        context.write(subscriptionKey, interactionCount);
    }
}
