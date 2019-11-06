import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Sorter calss can be used to sort a large file by spliting it into several temporary sorted files and
 * merging those temporary sorted files.
 * @author Arun Dhwaj
 */

public class FileSorter
{
    private final Comparator<String> sorter = new Comparator<String>()
    {
        @Override
        public int compare(String s, String t1)
        {
            return 0;
        }
    };

    // Chunk size: 1mb ===1000 000 bytes
    private int maxChunkSize = 1000000;

    //output=== outputFiles
    private List<File> outputFiles = new ArrayList<File>();
    private String tempDirectory = "tempOutput";

    /*
    public FileSorter(Comparator<String> sorter)
    {
        this.sorter = sorter;
    }
    */

    public void setTempDirectory(String tempDir)
    {
        this.tempDirectory = tempDir;
        File file = new File(tempDirectory);

        if ( !file.exists() || !file.isDirectory() )
        {
            throw new IllegalArgumentException("Parameter tempDir is not a directory or does not exist");
        }
    }

    /**
     * Sets the chunck size for temprary files
     * @param size
     */
    public void setMaximumChunkSize(int size)
    {
        this.maxChunkSize = size;
    }

    /**
     * Reads the input io stream and splits it into sorted chunks which are written to temporary files.
     * @param in
     * @throws IOException
     */
    public void splitChunks(InputStream in) throws IOException
    {
        outputFiles.clear();
        BufferedReader br = null;
        List<String> lines = new ArrayList<String>();

        try
        {
            br = new BufferedReader(new InputStreamReader(in));
            String line = null;
            int currChunkSize = 0;

            while ((line = br.readLine() ) != null )
            {
                lines.add(line);
                currChunkSize += line.length() + 1;

                if ( currChunkSize >= maxChunkSize )
                {
                    currChunkSize = 0;
                    Collections.sort(lines, sorter);

                    File file = new File(tempDirectory + "temp" + System.currentTimeMillis());
                    outputFiles.add(file);

                    writeOut(lines, new FileOutputStream(file));
                    lines.clear();
                }
            }

            //write out the remaining chunk
            Collections.sort(lines, sorter);
            File file = new File(tempDirectory + "temp" + System.currentTimeMillis());
            outputFiles.add(file);

            writeOut(lines, new FileOutputStream(file));
            lines.clear();

        }
        catch(IOException io)
        {
            throw io;
        }
        finally
        {
            if ( br != null )
            {
                try
                {
                    br.close();
                }
                catch (Exception e)
                {
                }
            }
        }
    }

    /**
     * Writes the list of lines out to the output stream, append new lines after each line.
     * @param list
     * @param os
     * @throws IOException
     * */
    private void writeOut(List<String> list, OutputStream os) throws IOException
    {
        BufferedWriter writer = null;
        try
        {
            writer = new BufferedWriter(new OutputStreamWriter(os));
            for ( String s : list )
            {
                writer.write(s);
                writer.write("\n");
            }
            writer.flush();

        }
        catch(IOException io)
        {
            throw io;
        }
        finally
        {
            if ( writer != null )
            {
                try
                {
                    writer.close();
                }
                catch(Exception e)
                {
                }
            }
        }
    }

    /**
     * Reads the temporary files created by splitChunks method and merges them in a sorted manner into the output stream.
     * @param list
     * @param os
     * @throws IOException
     */
    public void mergeChunks(OutputStream os) throws IOException
    {
        Map<StringWrapper, BufferedReader> map = new HashMap<StringWrapper, BufferedReader>();
        List<BufferedReader> readers = new ArrayList<BufferedReader>();

        BufferedWriter writer = null;
        ComparatorDelegate delegate = new ComparatorDelegate();

        try
        {
            writer = new BufferedWriter(new OutputStreamWriter(os));

            for ( int i = 0; i < outputFiles.size(); i++ )
            {
                BufferedReader reader = new BufferedReader(new FileReader(outputFiles.get(i)));
                readers.add(reader);
                String line = reader.readLine();

                if ( line != null )
                {
                    map.put(new StringWrapper(line), readers.get(i));
                }
            }

            ///continue to loop until no more lines lefts
            List<StringWrapper> sorted = new LinkedList<StringWrapper>(map.keySet());

            while ( map.size() > 0 )
            {
                Collections.sort(sorted, delegate);
                StringWrapper line = sorted.remove(0);

                writer.write(line.string);
                writer.write("\n");

                BufferedReader reader = map.remove(line);
                String nextLine = reader.readLine();

                if ( nextLine != null )
                {
                    StringWrapper sw = new StringWrapper(nextLine);
                    map.put(sw,  reader);
                    sorted.add(sw);
                }
            }
        }
        catch(IOException io)
        {
            throw io;
        }
        finally
        {
            for ( int i = 0; i < readers.size(); i++ )
            {
                try
                {
                    readers.get(i).close();
                }
                catch(Exception e){}
            }

            for ( int i = 0; i < outputFiles.size(); i++ )
            {
                outputFiles.get(i).delete();
            }

            try
            {
                writer.close();
            }
            catch(Exception e){}
        }
    }

    /**
     *  Delegate comparator to be able to sort the StringWrapper class. Delegates its behavior to
     * the sorter field.
     * @author Arun Dhwaj
     */
    private class ComparatorDelegate implements Comparator<StringWrapper>
    {
        @Override
        public int compare(StringWrapper o1, StringWrapper o2)
        {
            return sorter.compare(o1.string, o2.string);
        }
    }

    /**
     * Class which is a wrapper class for a String. This is necessary for String duplicates, which may cause equals/hashCode
     * conflicts within the HashMap used in the file merge.
     * @author Arun Dhwaj
     */
    public class StringWrapper implements Comparable<StringWrapper>
    {
        private final String string;

        public StringWrapper(String line)
        {
            this.string = line;
        }

        @Override
        public int compareTo(StringWrapper o)
        {
            return string.compareTo(o.string);
        }
    }
}
