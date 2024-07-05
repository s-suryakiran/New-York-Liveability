import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RideableTypeMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split CSV line into fields
        String[] fields = value.toString().split(",");
        // Skip header or invalid line
        if (fields.length > 1 && !fields[0].equals("ride_id")) {
            String endStationId = fields[7];
            String rideableType = fields[1];
            context.write(new Text(endStationId), new Text(rideableType));
        }
    }
}