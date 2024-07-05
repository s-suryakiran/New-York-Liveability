import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AggregateZipsReducer
  extends Reducer<Text, Text, Text, Text> {
  
  @Override
  public void reduce(Text key, Iterable<Text> values,
      Context context)
      throws IOException, InterruptedException {

    String keyValue = key.toString();
    int curCounter = 0;
    int destCounter = 0;
    int originCounter = 0;
    boolean shelter = false;
    for (Text value : values) {
        String[] row = value.toString().split(",");
	if (row.length == 3) {
	int curBusLocations = Integer.parseInt(row[0]);
	int destBusLocations = Integer.parseInt(row[1]);
	int originBusLocations = Integer.parseInt(row[2]);

	    curCounter += curBusLocations;
	    destCounter += destBusLocations;
	    originCounter += originBusLocations;
	}
	else if (row.length == 1) {
	    shelter = true;
            int curShelterLocations = Integer.parseInt(row[0]);
	    curCounter += curShelterLocations;
	}
    }

    if (shelter)
        context.write(key, new Text(String.valueOf(curCounter)));
    else
        context.write(key, new Text(String.valueOf(curCounter) + "," + String.valueOf(destCounter) + "," + String.valueOf(originCounter)));
    }
 }

