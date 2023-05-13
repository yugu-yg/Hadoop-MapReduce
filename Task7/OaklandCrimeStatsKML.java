/**
 * @Author: Yu Gu
 * Andrew ID: ygu3
 */

package edu.cmu.andrew.mm6;

import java.io.IOException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.NullWritable;
import java.util.Objects;

public class OaklandCrimeStatsKML extends Configured implements Tool {

    // Mapper class for the OaklandCrimeStatsKML class which inputs key:value pair in LongWritable and
    // Text and output key:value pair in Text and Text
    public static class OaklandCrimeStatsKMLMap extends Mapper<LongWritable, Text, Text, Text>
    {
        /**
         * Function to perform Hadoop's map operation. It finds the coordinates of the
         * crimes that were aggravated assault crimes within 350 meters of 3803 Forbes
         * Avenue in Oakland and assigns each one of them a value of a sample text.
         * @param key LongWritable key as input to the map function
         * @param value Text as value input to the map function
         * @param context Context to form output of map function
         * @throws IOException Exception while performing IO operations
         * @throws InterruptedException Exception during interrupted exception
         */
        // Source: https://stackoverflow.com/questions/16198752/advantages-of-using-nullwritable-in-hadoop
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            // Convert value to string
            String line = value.toString();

            // If it is not the first line in the output and the crime is an AGGRAVATED ASSAULT
            if (!Objects.equals(line.split("\t")[0], "X") && Objects.equals(line.split("\t")[4], "AGGRAVATED ASSAULT")) {

                // Get the x and y coordinates
                double x1 = Double.parseDouble(line.split("\t")[0]);
                double y1 = Double.parseDouble(line.split("\t")[1]);

                // Compute distance of x1 and y1 with 3803 Forbes Avenue in Oakland
                double dist = Math.sqrt(Math.pow((1354326.897 - x1), 2) + Math.pow((411447.7828 - y1), 2));

                // If the distance (after converting from feet to meter) is less than 350
                if ((dist * 0.3048) < 350) {

                    // Set context - Key:Value pair, where key is a sample text and value is
                    // the latitude and longitude of the crime
                    context.write(new Text("Location Coordinates"), new Text(line.split("\t")[8] + "," + line.split("\t")[7] + ",0"));
                }
            }
        }
    }

    // Reducer class for the OaklandCrimeStatsKML class which inputs key:value pair in Text and
    // Text and output key:value pair in NullWritable and Text
    public static class OaklandCrimeStatsKMLReducer extends Reducer<Text, Text, NullWritable, Text>
    {
        /**
         * Function to perform Hadoop's reduce operation. It gets the key:value pairs from the
         * mapper function, reduces the output from mapper to form the KML file corresponding to
         * the coordinates of the aggravated assault crimes within 350 meters of 3803 Forbes
         * Avenue in Oakland
         * @param key Text as input key to the reduce function
         * @param values Iterable<Text> as values to the reduce function
         * @param context Context to form output of reduce function
         * @throws IOException Exception while performing IO operations
         * @throws InterruptedException Exception during interrupted exception
         */
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            // Initialize KML string
            String kml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<kml xmlns=\"http://www.opengis.net/kml/2.2\">\n<Document>";

            // Loop over values
            for(Text value: values)
            {
                // Convert value to string
                String location = value.toString();

                // Insert location coordinates in the KML string
                kml = kml + "\n<Placemark>\n<description>" +  "Aggravated assault within 350 meters of 3803 Forbes Avenue in Oakland. Location: " +  location + "</description>\n<Point>\n<coordinates>" + location + "</coordinates>\n</Point>\n</Placemark>";
            }

            // End KML string
            kml = kml + "\n</Document>\n</kml>";

            // Set context as Key:Value pair storing the content of the KML file which contains the
            // coordinates of the aggravated assault crimes within 350 meters of 3803 Forbes Avenue
            // in Oakland in input file
            context.write(NullWritable.get(), new Text(kml));
        }
    }

    /**
     * Function to run the Hadoop map reduce operations. It also sets
     * various configurations required for the execution
     * @param args Input arguments to the Map Reduce function
     * @return Status code of the execution - 1 = Success; 0 = Failure
     * @throws Exception Exception if it occurs during execution
     */
    public int run(String[] args) throws Exception  {

        // Create a new job object
        Job job = new Job(getConf());

        // Set class for the job
        job.setJarByClass(OaklandCrimeStatsKML.class);

        // Set name of job
        job.setJobName("oaklandcrimestatskml");

        // Set output key's class
        job.setOutputKeyClass(NullWritable.class);

        // Set output value's class
        job.setOutputValueClass(Text.class);

        // Set mapper class
        job.setMapperClass(OaklandCrimeStatsKMLMap.class);

        // Set reducer class
        job.setReducerClass(OaklandCrimeStatsKMLReducer.class);

        // Set Mappers output key class
        job.setMapOutputKeyClass(Text.class);

        // Set Mappers output value class
        job.setMapOutputValueClass(Text.class);

        // Set input format class
        job.setInputFormatClass(TextInputFormat.class);

        // Set output format class
        job.setOutputFormatClass(TextOutputFormat.class);

        // Set input file path
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // Set output file path
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Stores a boolean value stating if the program executed successfully
        boolean success = job.waitForCompletion(true);

        // Return program execution status
        return success ? 0: 1;
    }


    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub

        // Run the Hadoop map reduce operation, store the status of program execution and display it
        int result = ToolRunner.run(new OaklandCrimeStatsKML(), args);
        System.exit(result);
    }
}