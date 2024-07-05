import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class StoreProfileMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text word = new Text();
    private Text outputValue = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = parseCSV(line);

        if (columns.length >= 4) {
            
            context.write(new Text("Total"), new Text("Count"));

            
            emitProfile(context, "ZipCode", columns[0].trim());

            
            emitProfile(context, "Borough", columns[1].trim().toLowerCase());

            
            emitProfile(context, "Year", columns[3].trim());
        }
    }

    private void emitProfile(Context context, String prefix, String value) throws IOException, InterruptedException {
        if (!value.isEmpty()) {
            context.write(new Text(prefix + "_Distinct"), new Text(value));
            if (!prefix.equals("Borough")) {  
                context.write(new Text(prefix + "_Min"), new Text(value));
            }
        }
    }

    private String[] parseCSV(String line) {
        return line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
    }
}
