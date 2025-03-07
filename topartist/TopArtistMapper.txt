package com.mycompany.topartist;


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Mapper;

public class TopArtistMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text yearArtistKey = new Text();
    private IntWritable one = new IntWritable(1);

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");
        
        if (fields.length > 0) {
            try {
                String timestamp = fields[12]; // Timestamp for year extraction
                String artist = fields[8];     // Artist name
                String action = fields[10];    // Action (Play/Skip/Like/etc.)
                
                String year = timestamp.split("/")[2].substring(0, 4); // Extract year

                // Create composite keys for both "Play" and "Skip"
                yearArtistKey.set(year + "_" + artist);
                
                // If action is "Play", emit (year_artist, 1)
                if ("Play".equals(action)) {
                    context.write(yearArtistKey, new IntWritable(1)); 
                }
                
                // If action is "Skip", emit (year_artist_skip, 1)
                if ("Skip".equals(action)) {
                    yearArtistKey.set(year + "_" + artist + "_skip"); // Adding "_skip" to differentiate
                    context.write(yearArtistKey, new IntWritable(1));
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
