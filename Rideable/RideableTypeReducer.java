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

public class RideableTypeReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<String, Integer> typeCounts = new HashMap<>();

        for (Text value : values) {
            String rideableType = value.toString();
            typeCounts.put(rideableType, typeCounts.getOrDefault(rideableType, 0) + 1);
        }

        for (Map.Entry<String, Integer> entry : typeCounts.entrySet()) {
            String compositeKey = key.toString() + "_" + entry.getKey();
            context.write(new Text(compositeKey), new Text(String.valueOf(entry.getValue())));
        }
    }
}