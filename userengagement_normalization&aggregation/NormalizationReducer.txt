package com.mycompany.userengagement_job1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class NormalizationReducer extends Reducer<Text, IntWritable, Text, Text> {
    @Override
protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    int totalInteractions = 0;
    Map<Integer, Integer> interactionCounts = new HashMap<>();

    // Calculate total interactions and frequencies
    for (IntWritable value : values) {
        int count = value.get();
        totalInteractions += count;
        interactionCounts.put(count, interactionCounts.getOrDefault(count, 0) + 1);
    }

    // Find the maximum interaction count 
    //Finds the maximum interaction count in the stream else 1
    int maxInteraction = interactionCounts.keySet().stream().max(Integer::compare).orElse(1);

    // Generate normalized scores
    StringBuilder result = new StringBuilder();
    for (Map.Entry<Integer, Integer> entry : interactionCounts.entrySet()) {
        int interactionCount = entry.getKey();
        int frequency = entry.getValue();
        double normalizedScore = ((double) interactionCount / totalInteractions) * 100;

        // Optionally use relative normalization
        // double relativeScore = ((double) interactionCount / maxInteraction) * 100;

        result.append("Interaction Count: ").append(interactionCount)
              .append(", Normalized Score: ").append(String.format("%.5f", normalizedScore)).append("%")
              .append(" (Frequency: ").append(frequency).append(")\t");
    }

    context.write(key, new Text(result.toString()));
}
}