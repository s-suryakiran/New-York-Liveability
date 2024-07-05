import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BusLocationReducer
  extends Reducer<Text, Text, Text, Text> {
  
  @Override
  public void reduce(Text key, Iterable<Text> values,
      Context context)
      throws IOException, InterruptedException {

    String keyValue = key.toString();
    double keyLat = Double.parseDouble(keyValue.split(",")[0]);
    double keyLong = Double.parseDouble(keyValue.split(",")[1]);
    int curCounter = 0;
    int destCounter = 0;
    int originCounter = 0;
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
          if (realValue.charAt(0) == 'C')
              curCounter += 1;
	  if (realValue.charAt(0) == 'D')
              destCounter += 1;
	  if (realValue.charAt(0) == 'O')
	      originCounter += 1;
      }
    }
    if (!newKey.equals(key.toString())){
    if (curCounter > 10 || destCounter > 10 || originCounter > 10)
        context.write(new Text(newKey), new Text(String.valueOf(curCounter) + "," + String.valueOf(destCounter) + "," + String.valueOf(originCounter)));}
  }
}
