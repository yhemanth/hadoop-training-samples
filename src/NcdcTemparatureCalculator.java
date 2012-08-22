import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NcdcTemparatureCalculator {

    private static Map<String, List<Integer>> temparaturesByYear = new HashMap<String, List<Integer>>();

    public static void main(String[] args) throws IOException {
        FileSystem fileSystem = FileSystem.get(new Configuration());
        Path path = new Path(args[0]);
        FSDataInputStream inputStream = fileSystem.open(path);
        int numRecords = 5;
        for (int i=0; i<numRecords; i++) {
            String record = readRecord(inputStream);
            processRecord(record);
        }
        findAverageTemparature();
        inputStream.close();
        fileSystem.close();
    }

    private static void findAverageTemparature() {
        for (String year : temparaturesByYear.keySet()) {
            List<Integer> temps = temparaturesByYear.get(year);
            int sum = 0;
            for (Integer temp : temps) {
                sum += temp.intValue();
            }
            System.out.println(year + ": " + sum/temps.size());
        }
    }

    private static void processRecord(String record) {
        String year = record.substring(15, 19);
        System.out.println(year);
        String temparature = record.substring(88, 93);
        System.out.println(temparature);
        List<Integer> temps = null;
        if (!temparaturesByYear.containsKey(year)) {
            temps = new ArrayList<Integer>();
            temparaturesByYear.put(year, temps);
        } else {
            temps = temparaturesByYear.get(year);
        }
        temps.add(Integer.parseInt(temparature));
    }

    private static String readRecord(FSDataInputStream inputStream) throws IOException {
        int recordLen = 105;
        byte[] record = new byte[recordLen];
        int bytesRead = 0;
        bytesRead = inputStream.read(record, 0, recordLen);
        while (bytesRead < recordLen) {
            bytesRead = inputStream.read(record, bytesRead, recordLen-bytesRead);
        }
        return new String(record);
    }
}
