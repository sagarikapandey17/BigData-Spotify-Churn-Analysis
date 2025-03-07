package Mappers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Top10SongsByRegionMapper extends Mapper<Object, Text, Text, Text> {
    private Text country = new Text();
    private Text songId = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split the row 
        String[] fields = value.toString().split(",");
        
        // Check if the row has sufficient columns 
        if (fields.length >= 12) {
            
            String action = fields[10];
            
            if ("Play".equals(action)) {
                country.set(fields[3]); // Set country
                songId.set(fields[6]); // Set songId
                context.write(country, songId); // Emit the country and songId pair
            }
        }
    }
}
