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

public class SubwayDataCleaningMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private Text stationInfo = new Text();
    private List<List<String>> zipCodesData; 
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
                if (parts.length >= 4) {
                    List<String> data = new ArrayList<>();
                    data.add(parts[0]); // ZIP code
                    data.add(parts[1]); // Latitude
                    data.add(parts[2]); // Longitude
                    data.add(parts[3]); // Borough
                    zipCodesData.add(data);
                }
            }
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

        if (columns.length < 15) return; 

        String stopName = columns[5].trim();
        String latitude = columns[9].trim();
        String longitude = columns[10].trim();

        
        if (latitude.isEmpty() || longitude.isEmpty()) return;

        double lat = Double.parseDouble(latitude);
        double lon = Double.parseDouble(longitude);

        String borough = convertBorough(columns[6].trim().toLowerCase());
        if (borough.equals("unknown")) {
            borough = estimateBoroughFromZip(findNearestZipCode(lat, lon));
        }

        String zipCode = findNearestZipCode(lat, lon);  

        String output = String.join(",", zipCode, stopName, borough, latitude, longitude);
        stationInfo.set(output);
        context.write(NullWritable.get(), stationInfo);
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

    private String estimateBoroughFromZip(String zipCode) {
        for (List<String> data : zipCodesData) {
            if (data.get(0).equals(zipCode)) {
                return data.get(3).toLowerCase(); 
            }
        }
        return "unknown";
    }

    private double haversine(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                   Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                   Math.sin(dLon / 2) * Math.sin(dLon / 2);
        return 2 * EARTH_RADIUS * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }

    private String convertBorough(String abbrev) {
        switch (abbrev) {
            case "bx": return "bronx";
            case "b":
            case "bk": return "brooklyn";
            case "m": return "manhattan";
            case "q": return "queens";
            case "si": return "staten island";
            default: return "unknown";  
        }
    }
}
