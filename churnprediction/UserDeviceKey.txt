package com.mycompany.churnprediction;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserDeviceKey implements WritableComparable<UserDeviceKey> {

    private String userId;
    private String device;
    private String timestamp;
   // private Text action;  

    // Default constructor
    public UserDeviceKey() {
    //    this.action = new Text(); // Initialize action
    this.userId = "";    // Initialize userId with an empty string
        this.device = "";    // Initialize device with an empty string
        this.timestamp = ""; // Initialize timestamp with an empty string
    }

    // Constructor with userId, device, timestamp, and action
    public UserDeviceKey(String userId, String device, String timestamp) {
        this.userId = userId;
        this.device = device;
        this.timestamp = timestamp;
        //this.action = new Text(action);
    }

    // Getters and setters
    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    //public Text getAction() {
    //    return action;
   // }

    //public void setAction(String action) {
    //    this.action = new Text(action);
    //}

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(userId);
        out.writeUTF(device);
        out.writeUTF(timestamp);
       // action.write(out); // Write action
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.userId = in.readUTF();
        this.device = in.readUTF();
        this.timestamp = in.readUTF();
        //action.readFields(in); // Read action
    }

    @Override
    public int compareTo(UserDeviceKey o) {
        int result = this.userId.compareTo(o.userId);
        if (result == 0) {
            result = this.timestamp.compareTo(o.timestamp);  // Sorting by timestamp
        }
        return result;
    }

    @Override
    public String toString() {
        return userId + "\t" + device + "\t" + timestamp; //+ action.toString(); // Include action in toString
    }
}
