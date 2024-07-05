import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import javax.naming.Context;

public class NYPDDataCleaning {

    public static class DataCleaningMapper extends Mapper<Object, Text, NullWritable, Text> {

        private List<List<String>> zipCodesData;
        private static final double EARTH_RADIUS = 6371; // in kilometers

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path path = new Path(cacheFiles[0]);
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
                zipCodesData = new ArrayList<>();
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(",");
                    if (parts.length >= 4) {
                        List<String> data = new ArrayList<>();
                        data.add(parts[0]); // ZIP code
                        data.add(parts[1]); // Latitude
                        data.add(parts[2]); // Longitude
                        data.add(parts[3]); // Borough
                        zipCodesData.add(data);
                    }
                }
                br.close();
            }
        }
        private String findNearestZipCode(double lat, double lon) {
            String nearestZip = "";
            double minDistance = Double.MAX_VALUE;
            for (List<String> zipData : zipCodesData) {
                double entryLat = Double.parseDouble(zipData.get(1));
                double entryLon = Double.parseDouble(zipData.get(2));
                double dist = haversine(lat, lon, entryLat, entryLon);
                if (dist < minDistance) {
                    minDistance = dist;
                    nearestZip = zipData.get(0);
                }
            }
            return nearestZip;
        }

        private double haversine(double lat1, double lon1, double lat2, double lon2) {
            double dLat = Math.toRadians(lat2 - lat1);
            double dLon = Math.toRadians(lon2 - lon1);
            double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                       Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                       Math.sin(dLon / 2) * Math.sin(dLon / 2);
            return 2 * EARTH_RADIUS * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        }
    

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] columns = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);

            try {
                String boro = columns[13].trim();
                String lawCatCd = columns[12].trim();
                String reportDate = columns[6].trim();
                String latitude = columns[27].trim();
                String longitude = columns[28].trim();
            

                // Check if any field is empty or null
                if (!boro.isEmpty() && !lawCatCd.isEmpty() && !reportDate.isEmpty() && !latitude.isEmpty() && !longitude.isEmpty()) {
                    double lat = Double.parseDouble(latitude);
                    double lon = Double.parseDouble(longitude);
                    String nearestZip = findNearestZipCode(lat, lon);
                    String cleanedOutput = String.join(",", new String[]{boro, lawCatCd, reportDate, latitude, longitude,nearestZip});
                    context.write(NullWritable.get(), new Text(cleanedOutput));
                }
            } catch (ArrayIndexOutOfBoundsException e) {
                // Handle cases where columns are not as expected
                context.write(NullWritable.get(), new Text("Incorrect data format"));
            }
        }
    }

    public static class DataCleaningReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(NullWritable.get(), value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: nypddatacleaning <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "NYPD Data Cleaning");
        job.addCacheFile(new URI("hdfs:///user/ss16030_nyu_edu/common/zip_code.csv"));
        job.setJarByClass(NYPDDataCleaning.class);
        job.setMapperClass(DataCleaningMapper.class);
        job.setReducerClass(DataCleaningReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
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
