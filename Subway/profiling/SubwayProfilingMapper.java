import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class SubwayProfilingMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable one = new IntWritable(1);
    private Text textKey = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        if (parts.length > 4) { 
            String zipCode = parts[0].trim(); 
            String borough = parts[2].trim(); 

            textKey.set("ZIP-" + zipCode);
            context.write(textKey, one);

            textKey.set("BOROUGH-" + borough);
            context.write(textKey, one);
        }
    }
}
