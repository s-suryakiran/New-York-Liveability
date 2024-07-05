import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.fs.FileSystem;
import java.io.IOException;

public class NYPDDataProfiling {

    public static class DataProfilingMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] columns = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);

            try {
                String boro = columns[0].trim();
                String lawCatCd = columns[1].trim();
                String reportDate = columns[2].trim();

                if (!boro.isEmpty() && !lawCatCd.isEmpty() && !reportDate.isEmpty()) {
                    word.set("Borough: " + boro);
                    context.write(word, one);
                    String year = reportDate.split("/")[2];
                    word.set("Year: " + year);
                    context.write(word, one);

                    word.set("LawCatCd: " + lawCatCd);
                    context.write(word, one);
                }
            } catch (Exception e) {
                // Handle cases where parsing fails or array indexing is incorrect
            }
        }
    }

    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: dataprofiling <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "NYPD Data Profiling");
        job.setJarByClass(NYPDDataProfiling.class);
        job.setMapperClass(DataProfilingMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setNumReduceTasks(1);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        Path outputPath = new Path(args[1]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // true is for recursive deletion
        }
        FileOutputFormat.setOutputPath(job, outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
