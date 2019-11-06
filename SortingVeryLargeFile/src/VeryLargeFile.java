import java.io.*;
import java.util.Comparator;

public class VeryLargeFile
{
    public static void main(String[] args) throws IOException
    {
        System.out.println("Test: sorting File: ");
        File inputFile = new File("./sample1.txt");
        InputStream is = new FileInputStream(inputFile);

        FileSorter fs = new FileSorter();

        fs.setMaximumChunkSize(1000000);
        fs.setTempDirectory("MyOutFileDir");
        fs.splitChunks(is);
    }
}
