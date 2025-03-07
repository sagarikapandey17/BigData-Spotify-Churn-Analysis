import Mappers.Top10SongsByRegionMapper;
import Reducers.SongReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TopSongsJob {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top 10 Songs by Region");
        job.setJarByClass(TopSongsJob.class);

        job.setMapperClass(Top10SongsByRegionMapper.class);
        job.setReducerClass(SongReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0])); // Input Path
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output Path

        // Set output format (default TextOutputFormat is fine for CSV)
        job.setOutputFormatClass(TextOutputFormat.class);

        // Wait for job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
