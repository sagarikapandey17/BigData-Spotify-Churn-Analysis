package com.mycompany.reducejoins;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class InteractionJoinMapper extends Mapper<LongWritable, Text, Text, CustomWritable> {

    private Text outputKey = new Text();

   @Override
protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] fields = value.toString().split(",");
    {
        String songId = fields[6];
        String[] interactionData = {fields[1], fields[3], fields[10], fields[12]};  // userId, country, action, timestamp
        context.write(new Text(songId), new CustomWritable("interaction", interactionData));
    }
}
}
