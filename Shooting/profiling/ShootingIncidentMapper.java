import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ShootingIncidentMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",");
        if (line.length > 4) {  
            String zipCode = line[0].trim();
            String borough = line[1].trim();
            String year = line[2].trim();  

            // Emit ZIP code count
            word.set("ZIP-" + zipCode);
            context.write(word, one);

            // Emit borough count
            word.set("BOROUGH-" + borough);
            context.write(word, one);

            // Emit year count
            word.set("YEAR-" + year);
            context.write(word, one);
        }
    }
}
