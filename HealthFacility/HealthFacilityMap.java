import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import javax.naming.Context;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class HealthFacilityMap {

    public static class HealthFacilityMapper extends Mapper<Object, Text, Text, Text> {
        private boolean isFirstLine = true; // To skip the header if present

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Skip the header line
            if (isFirstLine) {
                isFirstLine = false;
                return;
            }

            // Assuming comma-delimited file, if not change accordingly
            String[] columns = value.toString().split(",");

            // Check if the row has the expected number of columns
            if (columns.length > 14) {
                // Check for missing critical values (for example in columns 'Facility ID' and
                // 'Facility Address 1')
                if (!columns[0].trim().isEmpty() && !columns[5].trim().isEmpty()) {
                    // Emit key-value pair
                    context.write(new Text(columns[0].trim()), // Facility ID
                            new Text(columns[5].trim() + "_" + columns[9].trim() + "_" + columns[13].trim() + "_"
                                    + columns[14].trim())); // Address components
                }
            }
        }
    }

    // Reducer Class
    public static class HealthFacilityReducer extends Reducer<Text, Text, NullWritable, Text> {

        private Pattern pattern = Pattern.compile("^(.+?)_(\\d{5}(\\d{4})?)_(\\d+)_([A-Za-z ]+)$");

        // Reduce method
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // Initialize an empty String to hold the composite value
            String compositeValue = "";
            Matcher matcher;

            // Loop through values to remove duplicates and build the composite value
            for (Text value : values) {
                // Assuming there are no duplicates since Facility ID is unique, but logic for
                // duplicate check can be added
                compositeValue = value.toString();
                matcher = pattern.matcher(compositeValue);
                // Write the result
                if (matcher.matches()) {
                    context.write(NullWritable.get(), new Text(compositeValue));
                }
                break; // Assuming only one entry per Facility ID, otherwise add logic for duplicate
                       // check
            }
        }
    }

    // Main method to set up the MapReduce job
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: HealthFacilityMap <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Health Facility Map Data Profiling and Cleaning");

        job.setJarByClass(HealthFacilityMap.class);
        job.setMapperClass(HealthFacilityMapper.class);
        job.setReducerClass(HealthFacilityReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
