import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BusLocationMapper
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
            String recorededTime = row[0];
            int directionRef = Integer.parseInt(row[1]);
            String publishedName = row[2];
            String originName = row[3];
            double originLat = Double.parseDouble(row[4]);
            originLat -= originLat % 0.005d;
            double originLong = Double.parseDouble(row[5]);
            originLong -= originLong % 0.005d;
            String destinationName = row[6];
            double destinationLat = Double.parseDouble(row[7]);
            destinationLat -= destinationLat % 0.005d;
            double destinationLong = Double.parseDouble(row[8]);
            destinationLong -= destinationLong % 0.005d;
            // Omit vehicle id, we do not care for NYC id of a bus
            double vehicleLat = Double.parseDouble(row[10]);
            vehicleLat -= vehicleLat % 0.005d;
            double vehicleLong = Double.parseDouble(row[11]);
            vehicleLong -= vehicleLong % 0.005d;
            // Omit bus state status information (at stop/approaching) etc.
            String expectedArrivalTime = row[14];
            String actualArrivalTime = row[15];
            context.write(new Text(String.valueOf(vehicleLat) + "," + String.valueOf(vehicleLong)), new Text("C"));
	    context.write(new Text(String.valueOf(destinationLat) + "," + String.valueOf(destinationLong)), new Text("D"));
	    context.write(new Text(String.valueOf(originLat) + "," + String.valueOf(originLong)), new Text("O"));
        }
        catch(Exception E) {
            return;
        }
    }
  }
}
