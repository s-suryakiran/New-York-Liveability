import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class StoreProfileReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if (key.toString().contains("Distinct")) {
            HashSet<String> distinctSet = new HashSet<>();
            for (Text val : values) {
                distinctSet.add(val.toString());
            }
            if (key.toString().startsWith("Borough")) {
                result.set("Distinct Boroughs: " + distinctSet.size() + ", Values: " + distinctSet);
            } else {
                result.set("Distinct Count: " + distinctSet.size());
            }
            context.write(new Text(key.toString().split("_")[0]), result);
        } else if (key.toString().contains("Min")) {
            String min = null;
            String max = null;
            for (Text val : values) {
                String current = val.toString();
                if (min == null || current.compareTo(min) < 0) {
                    min = current;
                }
                if (max == null || current.compareTo(max) > 0) {
                    max = current;
                }
            }
            context.write(new Text(key.toString().split("_")[0] + " Min"), new Text(min));
            context.write(new Text(key.toString().split("_")[0] + " Max"), new Text(max));
        }
    }
}
