import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BusShelterLocationReducer
  extends Reducer<Text, Text, Text, Text> {
  
  @Override
  public void reduce(Text key, Iterable<Text> values,
      Context context)
      throws IOException, InterruptedException {

    String keyValue = key.toString();
    double keyLat = Double.parseDouble(keyValue.split(",")[0]);
    double keyLong = Double.parseDouble(keyValue.split(",")[1]);
    int counter = 0;
    double squareDist = 1000000d;
    String newKey = key.toString();
    for (Text value : values) {
      String realValue = value.toString();
      if (realValue.length() >= 5){
	String[] row = realValue.split(",");
        double zipLat = Double.parseDouble(row[1]);
	double zipLong = Double.parseDouble(row[2]);
	double squareDiff = (zipLat - keyLat) * (zipLat - keyLat) + (zipLong - keyLong) * (zipLong - keyLong);
	if (squareDiff < squareDist) {
	    newKey = row[0];
	    squareDist = squareDiff;
	}
      }
      else {
          counter += 1;
      }
    }
    if (counter >= 1)
        context.write(new Text(newKey), new Text(String.valueOf(counter)));
  }
}
