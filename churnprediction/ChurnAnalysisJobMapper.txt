package com.mycompany.churnprediction;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.logging.Logger;

public class ChurnAnalysisJobMapper extends Mapper<Object, Text, UserDeviceKey, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        // CSV format: User ID, Device, Timestamp, Action
        String[] fields = value.toString().split(",");
        
        if (fields.length >= 12) {
            String userId = fields[1];
            String device = fields[11];
            String timestamp = fields[12];
            String action = fields[10];

            // Create a UserDeviceKey with userId, device, timestamp, and action
            if (userId != null && device != null && timestamp != null) {
            UserDeviceKey userDeviceKey = new UserDeviceKey(userId, device, timestamp);

            // Emit the composite key (user, device, timestamp) and the action as value
            //System.out.print("Emitting key: " + userId+device+timestamp + ", value: " + new Text(action));
            context.write(userDeviceKey, new Text(action));
            }
        }
    }
    
    }

