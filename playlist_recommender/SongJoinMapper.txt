package com.mycompany.reducejoins;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class SongJoinMapper extends Mapper<LongWritable, Text, Text, CustomWritable> {

    private Text outputKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] fields = value.toString().split(",");
  {
        String songId = fields[0];
        String[] songData = {fields[1], fields[2], fields[4], fields[6]};  // title, artist, genre, popularity
        context.write(new Text(songId), new CustomWritable("song", songData));
    }
}
}
