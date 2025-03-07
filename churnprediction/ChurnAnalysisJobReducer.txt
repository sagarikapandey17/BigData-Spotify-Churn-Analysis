package com.mycompany.churnprediction;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ChurnAnalysisJobReducer extends Reducer<UserDeviceKey, Text, UserDeviceKey, Text> {

    private static final SimpleDateFormat sdf = new SimpleDateFormat("M/d/yy HH:mm");

    @Override
    protected void reduce(UserDeviceKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Current date
        Date currentDate = new Date();
        long latestActionTime = 0;

        // Iterate over all actions for this user-device pair
        for (Text value : values) {
            String timestamp = key.getTimestamp();
            try {
                // Parse the timestamp and compare it with the current latest
                long actionTime = sdf.parse(timestamp).getTime();
                if (actionTime > latestActionTime) {
                    latestActionTime = actionTime; // Update to the latest timestamp
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // Calculate difference in days from the current date
        long diffInMillis = currentDate.getTime() - latestActionTime;
        long diffInDays = diffInMillis / (1000 * 60 * 60 * 24);

        // Determine churn status based on diffInDays
        String status = (diffInDays > 200) ? "Churned" : "Active";

        // Emit the key-value pair where:
        // Key: UserDeviceKey (userId, device)
        // Value: The diffInDays and the churn status
        context.write(key, new Text(diffInDays + "\t" + status));
    }
}
