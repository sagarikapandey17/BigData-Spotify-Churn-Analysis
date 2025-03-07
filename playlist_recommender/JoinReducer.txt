package com.mycompany.reducejoins;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JoinReducer extends Reducer<Text, CustomWritable, Text, Text> {

    private Text outputValue = new Text();

    @Override
    
protected void reduce(Text key, Iterable<CustomWritable> values, Context context) throws IOException, InterruptedException {
    String[] songDetails = null;
    List<String[]> interactions = new ArrayList<>();

    // Iterate through all CustomWritable values
    for (CustomWritable val : values) {
        if ("song".equals(val.getType())) {
            // Use getter to retrieve fields for the song
            songDetails = val.getFields();
        } else if ("interaction".equals(val.getType())) {
            // Use getter to retrieve fields for interactions
            interactions.add(val.getFields());
        }
    }

    // If song details exist, join with interaction data
    if (songDetails != null) {
        for (String[] interaction : interactions) {
            // Combine interaction and song details into one string
            String result = String.join("\t", interaction) + "\t" + String.join("\t", songDetails);
            context.write(key, new Text(result));  // Emit the result
        }
    }
}
}


