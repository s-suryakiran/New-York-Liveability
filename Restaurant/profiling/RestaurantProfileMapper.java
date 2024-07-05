import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class RestaurantProfileMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        if (parts.length >= 5) {

            word.set("ZIP-" + parts[0].trim());
            context.write(word, one);

            word.set("BOROUGH-" + parts[2].trim().toLowerCase());
            context.write(word, one);
        }
    }
}
