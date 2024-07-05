import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class PreviewCSV {
    public static void main(String[] args) {
        String csvFile = "./facilities_23v2.csv";
        String line = "";
        int maxLines = 10; // Number of lines you want to preview

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            int count = 0;
            while ((line = br.readLine()) != null && count < maxLines) {
                System.out.println(line);
                count++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
