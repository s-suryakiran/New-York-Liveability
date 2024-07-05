import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.FileSystem;

public class DataCleanAir {

    public static class CleaningMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private Text word = new Text();
        private static final Pattern CSV_PATTERN = Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

        private String determineBorough(String placeName) {
            if (placeName.contains("Manhattan")) return "manhattan";
            if (placeName.contains("Brooklyn")) return "brooklyn";
            if (placeName.contains("Queens")) return "queens";
            if (placeName.contains("Bronx")) return "bronx";
            if (placeName.contains("Staten Island")) return "staten island";
            return null;
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = CSV_PATTERN.split(value.toString());
            if (line.length > 10) {
                String name = line[2].trim();
                String measureInfo = line[4].trim();
                String geoPlaceName = line[7].trim();
                String startDate = line[9].trim();
                String dataValueString = line[10].trim();
                String year = startDate.split("/").length == 3 ? startDate.split("/")[2] : "";
                String borough = determineBorough(geoPlaceName);

                if (!year.equals("Unknown") && !name.isEmpty() && !measureInfo.isEmpty() && !geoPlaceName.isEmpty() && !dataValueString.isEmpty()) {
                    try {
                        Double.parseDouble(dataValueString); // Validate if data value is a valid double
                        if (borough != null) {
                            String cleanRecord = String.join(",", borough, name, measureInfo, year, dataValueString);
                            word.set(cleanRecord);
                            context.write(NullWritable.get(), word);
                        }
                    } catch (NumberFormatException e) {
                        // Handle error or skip this record
                    }
                }
            }
        }
    }

    public static class CleaningReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(NullWritable.get(), value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Data Cleaning Job");
        job.setJarByClass(DataCleanAir.class);
        job.setMapperClass(CleaningMapper.class);
        job.setReducerClass(CleaningReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setNumReduceTasks(1);
        Path outputPath = new Path(args[1]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // Recursive deletion
        }
        FileOutputFormat.setOutputPath(job, outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
