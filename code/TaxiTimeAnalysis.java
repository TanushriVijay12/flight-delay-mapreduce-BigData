import java.io.IOException; 
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.*; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
 
// Main class to perform average taxi time analysis at airports 
public class TaxiTimeAnalysis { 
 
    // Mapper class: emits (airport_code, taxi_time) for both taxi out and taxi in 
times 
    public static class TaxiMapper extends Mapper<LongWritable, Text, Text, 
FloatWritable> { 
 
        private boolean headerSkipped = false; // To ensure we skip the header line 
        private Text airport = new Text();      // Key: airport code (origin or 
destination) 
        private FloatWritable taxiTime = new FloatWritable(); // Value: taxi time 
 
        @Override 
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException { 
            String line = value.toString(); 
 
            // Skip the header line 
            if (!headerSkipped) { 
                headerSkipped = true; 
                return; 
            } 
 
            // Split the CSV line into fields 
            String[] fields = line.split(",", -1); // -1 to include empty fields as empty 
strings 
 
            // Ignore invalid lines with fewer than 13 columns 
            if (fields.length < 13) return; 
 
            try { 
                // Extract important fields: origin, destination, taxi out, and taxi in 
                String originAirport = fields[2].trim();  // ORIGIN column 
                String destAirport = fields[3].trim();    // DEST column 
                String taxiOutStr = fields[4].trim();     // TAXI_OUT column 
                String taxiInStr = fields[5].trim();      // TAXI_IN column 
 
                // Emit (origin airport, taxi out time) if data is valid 
                if (!originAirport.isEmpty() && !taxiOutStr.isEmpty()) { 
                    float taxiOut = Float.parseFloat(taxiOutStr); 
                    airport.set(originAirport); 
                    taxiTime.set(taxiOut); 
                    context.write(airport, taxiTime); 
                } 
 
                // Emit (destination airport, taxi in time) if data is valid 
                if (!destAirport.isEmpty() && !taxiInStr.isEmpty()) { 
                    float taxiIn = Float.parseFloat(taxiInStr); 
                    airport.set(destAirport); 
                    taxiTime.set(taxiIn); 
                    context.write(airport, taxiTime); 
                } 
 
            } catch (Exception e) { 
                // If any parsing error happens, skip the current row 
            } 
        } 
    } 
 
    // Reducer class: calculates the average taxi time for each airport 
    public static class TaxiReducer extends Reducer<Text, FloatWritable, Text, 
FloatWritable> { 
 
        @Override 
        public void reduce(Text key, Iterable<FloatWritable> values, Context 
context) 
                throws IOException, InterruptedException { 
 
            float sum = 0;   // Sum of all taxi times 
            int count = 0;   // Count of taxi records 
 
            // Aggregate sum and count 
            for (FloatWritable v : values) { 
                sum += v.get(); 
                count++; 
            } 
 
            // Only write output if there were valid records 
            if (count > 0) { 
                float avgTaxiTime = sum / count; // Compute average 
                context.write(key, new FloatWritable(avgTaxiTime)); // Emit 
(airport_code, avgTaxiTime) 
            } 
        } 
    } 
 
    // Main method to configure and start the Hadoop job 
    public static void main(String[] args) throws Exception { 
        if (args.length != 2) { 
            System.err.println("Usage: TaxiTimeAnalysis <input path> <output 
path>"); 
            System.exit(-1); // Exit if incorrect number of arguments provided 
        } 
 
        Configuration conf = new Configuration(); 
        Job job = Job.getInstance(conf, "Taxi Time Analysis"); // Set job name 
 
        job.setJarByClass(TaxiTimeAnalysis.class); 
        job.setMapperClass(TaxiMapper.class); 
        job.setReducerClass(TaxiReducer.class); 
 
        // Define output types 
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(FloatWritable.class); 
 
        // Set input and output file paths 
        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1])); 
 
        // Submit the job and exit based on success/failure 
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    } 
} 