import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.naming.Context;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;

public class CitiBikeDataProfiler {

    public static class BikeDataMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Assuming the first line is the header and columns are ordered as in the
            String[] fields = value.toString().split(",");
            // Skip header or rows with missing critical columns
            if (fields.length > 8 && !fields[0].equals("ride_id") && !fields[7].isEmpty() && !fields[1].isEmpty()) {

                String compositeKey = fields[7] + "_" + fields[1]; // end_station_id_rideable_type
                String compositeValue = String.join("_",
                        fields[6], // end_station_name
                        fields[7], // end_station_id
                        fields[8], // start_lat
                        fields[9], // start_lng
                        fields[10], // end_lat
                        fields[11] // end_lng
                );
                context.write(new Text(compositeKey), new Text(compositeValue));
            }
        }
    }

    public static class BikeDataReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            Text details = new Text();
            for (Text value : values) {
                count++;
                details = value; // Assuming all values are the same for each unique key
            }
            String result = key.toString() + " = " + count + "_" + details.toString();
            context.write(null, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CitiBikeDataProfiler <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Citi Bike Data Profiling and Cleaning");
        job.setJarByClass(CitiBikeDataProfiler.class);

        job.setMapperClass(BikeDataMapper.class);
        job.setReducerClass(BikeDataReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean jobCompleted = job.waitForCompletion(true);
        if (jobCompleted) {
            // Define paths for the job output and the final CSV file
            Path jobOutputPath = new Path(args[1] + "/part-r-00000");
            Path csvOutputPath = new Path(args[1] + "_csv");

            // Initialize filesystem
            FileSystem fs = FileSystem.get(conf);

            // Perform the conversion to CSV format
            try (
                    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(jobOutputPath)));
                    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(csvOutputPath, true)))) {
                String line;
                // Write the header to the CSV file
                bw.write("end_station_id,rideable_type,count,end_station_name,start_lat,start_lng,end_lat,end_lng\n");

                while ((line = br.readLine()) != null) {
                    // Expected line format: 'end_station_id_rideable_type =
                    // count_end_station_name_start_lat_start_lng_end_lat_end_lng'
                    String[] parts = line.split(" = ");
                    if (parts.length == 2) {
                        String[] keys = parts[0].split("_");
                        String[] values = parts[1].split("_", 2);
                        if (keys.length == 2 && values.length == 2) {
                            // Properly reformatting the values by replacing underscores
                            String[] details = values[1].split("_");
                            if (details.length == 5) { // Ensuring there are exactly five parts
                                bw.write(String.format("%s,%s,%s,%s,%s,%s,%s,%s\n",
                                        keys[0], keys[1], values[0], details[0], details[1], details[2], details[3],
                                        details[4]));
                            }
                        }
                    }
                }
            } catch (IOException e) {
                System.err.println("Error while converting to CSV: " + e.getMessage());
                System.exit(-1);
            }
        }

        System.exit(jobCompleted ? 0 : 1);
    }
}