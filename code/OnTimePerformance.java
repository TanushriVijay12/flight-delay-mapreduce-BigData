import java.io.IOException; 
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.*; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
// Main class to perform on-time flight performance analysis 
public class OnTimePerformance { 
 
    // Mapper class: processes each line and emits (airline_code, on_time_flag) 
    public static class OnTimeMapper extends Mapper<LongWritable, Text, Text, 
IntWritable> { 
 
        private boolean headerSkipped = false; // To make sure we skip the first 
header line 
        private Text airlineCode = new Text();  // Key: airline code 
        private IntWritable onTimeFlag = new IntWritable(); // Value: 1 if on-time, 
0 otherwise 
 
        @Override 
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException { 
            String line = value.toString(); 
 
            // Skip the first line which is the header 
            if (!headerSkipped) { 
                headerSkipped = true; 
                return; 
            } 
 
            // Split the CSV line into fields 
            String[] fields = line.split(",", -1); // -1 to include empty fields also 
 
            // Ignore lines with insufficient columns 
            if (fields.length < 13) return; 
 
            try { 
                // Extract airline code (assuming it's in column 2) 
                String carrier = fields[1].trim(); 
                if (carrier.isEmpty()) return; // Skip if carrier code is missing 
 
                // Read delay columns safely (with fallback if missing or invalid) 
                float delayCarrier = parseFloatSafe(fields[7]); 
                float delayWeather = parseFloatSafe(fields[8]); 
                float delayNAS = parseFloatSafe(fields[9]); 
                float delaySecurity = parseFloatSafe(fields[10]); 
                float delayLateAircraft = parseFloatSafe(fields[11]); 
 
                // Calculate total delay 
                float totalDelay = delayCarrier + delayWeather + delayNAS + 
delaySecurity + delayLateAircraft; 
 
                // Rule: Flight is "on-time" if total delay is 5 minutes or less 
                int onTime = (totalDelay <= 5.0f) ? 1 : 0; 
 
                // Emit (airlineCode, onTimeFlag) 
                airlineCode.set(carrier); 
                onTimeFlag.set(onTime); 
 
                context.write(airlineCode, onTimeFlag); 
 
            } catch (Exception e) { 
                // If parsing fails, skip that record 
            } 
        } 
 
        // Helper function to safely parse floats 
        private float parseFloatSafe(String s) { 
            if (s == null || s.trim().isEmpty()) return 0.0f; 
            try { 
                return Float.parseFloat(s.trim()); 
            } catch (NumberFormatException e) { 
                return 0.0f; 
            } 
        } 
    } 
 
    // Reducer class: aggregates results for each airline 
    public static class OnTimeReducer extends Reducer<Text, IntWritable, Text, 
Text> { 
 
        @Override 
        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException { 
 
            int totalFlights = 0;   // Total number of flights for this airline 
            int onTimeFlights = 0;  // Number of on-time flights 
 
            // Sum up total flights and on-time flights 
            for (IntWritable v : values) { 
                totalFlights++; 
                onTimeFlights += v.get(); // Add 1 if on-time, 0 if delayed 
            } 
 
            // Calculate the on-time rate in percentage 
            float onTimeRate = (totalFlights == 0) ? 0 : (onTimeFlights * 100.0f) / 
totalFlights; 
 
            // Format the output nicely 
            String result = String.format("TotalFlights=%d, OnTimeFlights=%d, 
OnTimeRate=%.2f%%", 
                                          totalFlights, onTimeFlights, onTimeRate); 
 
            // Emit (airlineCode, formatted result) 
            context.write(key, new Text(result)); 
        } 
    } 
 
    // Main function to configure and run the job 
    public static void main(String[] args) throws Exception { 
        if (args.length != 2) { 
            System.err.println("Usage: OnTimePerformance <input path> <output 
path>"); 
            System.exit(-1); // Exit if input and output paths are not provided 
        } 
 
        Configuration conf = new Configuration(); 
        Job job = Job.getInstance(conf, "On-Time Performance Analysis"); // Job 
name 
 
        job.setJarByClass(OnTimePerformance.class); 
        job.setMapperClass(OnTimeMapper.class); 
        job.setReducerClass(OnTimeReducer.class); 
 
        // Set output key and value types 
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(IntWritable.class); 
 
        // Set input and output file paths 
        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1])); 
 
        // Wait for the job to finish and exit appropriately 
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    } 
} 