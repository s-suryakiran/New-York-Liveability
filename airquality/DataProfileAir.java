import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;

public class DataProfileAir {

    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text compositeKey = new Text();
        private final static Text missing = new Text("Missing");
        private static final Pattern CSV_PATTERN = Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Using regex pattern to split the CSV line
            String[] line = CSV_PATTERN.split(value.toString());
            if (line.length > 10) {
                String name = line[2].trim();
                String measureInfo = line[4].trim();
                String geoPlaceName = line[7].trim();
                String startDate = line[9].trim();
                String dataValueString = line[10].trim();
                String year = startDate.split("/").length == 3 ? startDate.split("/")[2] : "Unknown";

                // Append Measure Info to the year and borough
                if (!year.equals("Unknown")) {
                    context.write(new Text("Year_" + year + "_" + measureInfo), new Text(dataValueString));
                } else {
                    context.write(new Text("Missing_Year"), missing);
                }

                String borough = determineBorough(geoPlaceName);
                if (borough != null) {
                    context.write(new Text("Borough_" + borough + "_" + measureInfo), new Text(dataValueString));
                } else {
                    context.write(new Text("Missing_Borough"), missing);
                }
            }
        }

        private String determineBorough(String placeName) {
            if (placeName.contains("Manhattan")) return "Manhattan";
            if (placeName.contains("Brooklyn")) return "Brooklyn";
            if (placeName.contains("Queens")) return "Queens";
            if (placeName.contains("Bronx")) return "Bronx";
            if (placeName.contains("Staten Island")) return "Staten Island";
            return null;
        }
    }

    public static class DataProfileReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            double sum = 0;

            for (Text value : values) {
                if (!value.toString().equals("Missing")) {
                    try {
                        sum += Double.parseDouble(value.toString());
                        count++;
                    } catch (NumberFormatException e) {
                        context.write(new Text("Error_Parsing"), new Text(value.toString()));
                    }
                }
            }

            double mean = count > 0 ? sum / count : 0;
            result.set("Mean: " + mean);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Data Profiling Air Quality");
        job.setJarByClass(DataProfileAir.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(DataProfileReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        Path outputPath = new Path(args[1]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // true is for recursive deletion
        }
        FileOutputFormat.setOutputPath(job, outputPath);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
