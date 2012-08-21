import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class FsReader {

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("Usage: hadoop FsReader <filename>");
            System.exit(-1);
        }
        FileSystem fileSystem = FileSystem.get(new Configuration());
        Path path = new Path(args[0]);
        FSDataInputStream fsStream = fileSystem.open(path);
        byte[] buffer = new byte[1024];
        int bytesRead;
        while ((bytesRead = fsStream.read(buffer)) > 0) {
            System.out.write(buffer, 0, bytesRead);
        }
        fsStream.close();
        fileSystem.close();
    }
}
