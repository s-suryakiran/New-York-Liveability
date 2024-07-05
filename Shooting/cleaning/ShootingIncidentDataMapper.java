import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import java.net.URI;

public class ShootingIncidentDataMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private Text outputValue = new Text();
    private List<List<Double>> zipCodesData; // List to store zip code and coordinates

    // Earth radius in kilometers for Haversine distance calculation
    private static final double EARTH_RADIUS = 6371;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        URI[] cacheFiles = context.getCacheFiles(); 
        if (cacheFiles != null && cacheFiles.length > 0) {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path path = new Path(cacheFiles[0]);
            loadZipCodeData(fs, path);
        }
    }

    private void loadZipCodeData(FileSystem fs, Path path) throws IOException {
        zipCodesData = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length >= 3) {
                    List<Double> data = new ArrayList<>();
                    data.add(Double.parseDouble(parts[0])); // ZIP code
                    data.add(Double.parseDouble(parts[1])); // Latitude
                    data.add(Double.parseDouble(parts[2])); // Longitude
                    zipCodesData.add(data);
                }
            }
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split(",");
        if (columns.length < 20) return;

        String latitude = columns[18].trim();
        String longitude = columns[19].trim();
        if (latitude.isEmpty() || longitude.isEmpty()) return;

        double lat = Double.parseDouble(latitude);
        double lon = Double.parseDouble(longitude);
        String zipCode = findNearestZipCode(lat, lon);

        String borough = columns[3].trim().toLowerCase();
        String year = columns[1].trim().substring(6, 10);

        String combinedInfo = String.join(", ", zipCode, borough, year, latitude, longitude);
        outputValue.set(combinedInfo);
        context.write(NullWritable.get(), outputValue);
    }

    private String findNearestZipCode(double lat, double lon) {
        String nearestZip = "";
        double minDistance = Double.MAX_VALUE;
        for (List<Double> zipData : zipCodesData) {
            double dist = haversine(lat, lon, zipData.get(1), zipData.get(2));
            if (dist < minDistance) {
                minDistance = dist;
                nearestZip = String.valueOf(zipData.get(0).intValue());
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
}
