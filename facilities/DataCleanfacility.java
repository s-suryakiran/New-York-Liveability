import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import java.io.IOException;
import java.util.regex.Pattern;

public class DataCleanfacility {

    public static class CleanDataMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private static final Pattern CSV_PATTERN = Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
        private static final int ZIPCODE_IDX = 5;
        private static final int FACNAME_IDX = 0;
        private static final int BORO_IDX = 19;
        private static final int FACTYPE_IDX = 6;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = CSV_PATTERN.split(line, -1); // Use split with limit -1 to include empty trailing fields
            
            // Only process records with all required fields and sufficient data
            if (fields.length > 19 && isValid(fields)) {
                String zipCode = fields[ZIPCODE_IDX].trim();
                String facName = fields[FACNAME_IDX].trim();
                String boro = fields[BORO_IDX].trim().toLowerCase();
                String facType = fields[FACTYPE_IDX].trim();

                // Compose the cleaned record and write it to the context
                String cleanRecord = String.join(",", zipCode, boro, facName, facType);
                context.write(NullWritable.get(), new Text(cleanRecord));
            }
        }

        // Helper method to check if fields are not empty
        private boolean isValid(String[] fields) {
            return !fields[ZIPCODE_IDX].trim().isEmpty() &&
                   !fields[FACNAME_IDX].trim().isEmpty() &&
                   !fields[BORO_IDX].trim().isEmpty() &&
                   !fields[FACTYPE_IDX].trim().isEmpty();
        }
    }

    public static class CleanDataReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        @Override
        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(NullWritable.get(), value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Clean Data");
        job.setJarByClass(DataCleanfacility.class);
        job.setMapperClass(CleanDataMapper.class);
        job.setReducerClass(CleanDataReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // true is for recursive deletion
        }
        FileOutputFormat.setOutputPath(job, outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
