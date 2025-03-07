package com.mycompany.highvalueusers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HighValueUserGenreReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();
    private static final int THRESHOLD = 4;  // Define the threshold value

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        
        // Sum the values (each one represents a unique interaction by the user)
        for (IntWritable val : values) {
            sum += val.get();
        }

        // Only output the result if the sum is greater than the threshold
        if (sum > THRESHOLD) {
            result.set(sum);
            context.write(key, result); // Output user ID and total number of unique genre interactions
        }
    }
}
