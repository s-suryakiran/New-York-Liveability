import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.net.URI;

public class RestaurantDataCleaningJob {
    public static void main(String[] args) throws Exception {
        // Check that two arguments are passed for input and output directories
        if (args.length != 2) {
            System.err.println("Usage: RestaurantDataCleaningJob <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Restaurant Data Cleaning");

        job.addCacheFile(new URI("hdfs:///user/ar8006_nyu_edu/common/ziplatlong.csv"));

        job.setJarByClass(RestaurantDataCleaningJob.class);
        
        job.setMapperClass(RestaurantDataCleaningMapper.class);
        
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);

        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); 
        }

        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
