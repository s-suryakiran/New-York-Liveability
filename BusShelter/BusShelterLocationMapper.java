import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BusShelterLocationMapper
  extends Mapper<LongWritable, Text, Text, Text> {

  private static final int MISSING = 9999;
  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    String line = value.toString();
    String[] row = line.split(",");
    if (row.length == 3) {
    	int zip = Integer.parseInt(row[0]);
	double latitude = Double.parseDouble(row[1]);
	latitude -= latitude % 0.005d;
	double longitude = Double.parseDouble(row[2]);
	longitude -= longitude % 0.005d;

	for (int i = -19; i < 20; i++) {
	    for (int j = -19; j < 20; j++) {
	        context.write(new Text(String.valueOf(latitude + i * 0.005d) + "," + String.valueOf(longitude + j * 0.005d)), new Text(String.valueOf(zip) + "," + String.valueOf(latitude) + "," + String.valueOf(longitude)));
	    }
	}
    }
    else {
        try {
            double oLat = Double.parseDouble(row[13]);
            oLat -= oLat % 0.005d;
            double oLong = Double.parseDouble(row[12]);
            oLong -= oLong % 0.005d;
	    context.write(new Text(String.valueOf(oLat) + "," + String.valueOf(oLong)), new Text(""));
        }
        catch(Exception E) {
            return;
        }
    }
  }
}
