package com.mycompany.reducejoins;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomWritable implements Writable {

    private String type;  // "song" or "interaction"
    private String[] fields;

    // Default constructor
    public CustomWritable() {}

    // Parameterized constructor
    public CustomWritable(String type, String[] fields) {
        this.type = type;
        this.fields = fields;
    }

    public String getType() {
        return type;
    }

    public String[] getFields() {
        return fields;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(type);
        out.writeInt(fields.length);
        for (String field : fields) {
            out.writeUTF(field);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        type = in.readUTF();
        int length = in.readInt();
        fields = new String[length];
        for (int i = 0; i < length; i++) {
            fields[i] = in.readUTF();
        }
    }
}
