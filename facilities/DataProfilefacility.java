import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.HashSet;
import java.util.regex.Pattern;

public class DataProfilefacility {

    public static class ProfilingMapper extends Mapper<Object, Text, Text, Text> {
        private static final Pattern CSV_PATTERN = Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
        private boolean isHeader = true;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] dataArray = CSV_PATTERN.split(value.toString());
            if (dataArray.length > 19) {
                String zipCode = dataArray[5].trim();
                String facilityType = dataArray[7].trim();
                String borough = dataArray[19].trim();

                if (!zipCode.isEmpty()) {
                    context.write(new Text("UniqueZipCode"), new Text(zipCode));
                } else {
                    context.write(new Text("MissingZipCode"), new Text("1"));
                }

                if (!facilityType.isEmpty()) {
                    context.write(new Text("UniqueFacType"), new Text(facilityType));
                } else {
                    context.write(new Text("MissingFacType"), new Text("1"));
                }

                if (!borough.isEmpty()) {
                    context.write(new Text("UniqueBorough"), new Text(borough));
                    context.write(new Text("CountFacInBorough-" + borough), new Text("1"));
                } else {
                    context.write(new Text("MissingBorough"), new Text("1"));
                }
            } else {
                context.write(new Text("IncompleteRecord"), new Text("1"));
            }
        }
    }

    public static class SummaryReducer extends Reducer<Text, Text, Text, IntWritable> {
        private HashSet<String> uniqueValues = new HashSet<>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (key.toString().startsWith("Unique")) {
                uniqueValues.clear();
                for (Text val : values) {
                    uniqueValues.add(val.toString());
                }
                context.write(new Text(key.toString()), new IntWritable(uniqueValues.size()));
            } else if (key.toString().startsWith("CountFacInBorough")) {
                int sum = 0;
                for (Text val : values) {
                    sum += Integer.parseInt(val.toString());
                }
                context.write(key, new IntWritable(sum));
            } else {
                int sum = 0;
                for (Text val : values) {
                    sum += Integer.parseInt(val.toString());
                }
                context.write(new Text(key.toString()), new IntWritable(sum));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "data profiling");
        job.setJarByClass(DataProfilefacility.class);
        job.setMapperClass(ProfilingMapper.class);
        job.setReducerClass(SummaryReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
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

