import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class StoreDataCleaningMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private Text outputValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        
        
        String[] columns = parseCSV(line);

       
        if (columns.length < 9) {
            return; 
        }

        
        String zipcode = columns[4].trim();
        String borough = columns[3].trim().toLowerCase(); 
        String restaurantName = columns[0].trim();
        String yearAwarded = columns[5].trim();
        String latitude = columns[7].trim();
        String longitude = columns[8].trim();

       
        if (!isValidZipcode(zipcode) || !isValidYear(yearAwarded) || !isValidBorough(borough) ||
            zipcode.isEmpty() || (latitude.isEmpty() && longitude.isEmpty())) {
            return; 
        }

       
        outputValue.set(zipcode + "," + borough + "," + restaurantName + "," + yearAwarded);
        context.write(NullWritable.get(), outputValue);
    }

    private boolean isValidZipcode(String zipcode) {
        
        return zipcode.matches("1\\d{4}");
    }

    private boolean isValidYear(String year) {
        
        return year.matches("\\d{4}");
    }

    private boolean isValidBorough(String borough) {
        
        String[] validBoroughs = {"new york", "bronx", "brooklyn", "queens", "staten island"};
        for (String valid : validBoroughs) {
            if (valid.equals(borough)) {
                return true;
            }
        }
        return false;
    }

    private String[] parseCSV(String line) {
        
        return line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
    }
}