package eps.examples.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.LongWritable;

// Execution: hadoop jar BigData_Project.jar eps.examples.mapreduce.WorldUrbanization /user/am17/CSVs/world-urbanization-prospects-the-2018-revision-population.csv /user/am17/out
// This class will handle World Urbanization Prospect's CSV.
// It will return the prospect for the 1980, 2006 or 2020 and 2050 of every country (if the country has that information).
public class WorldUrbanization {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf);
        job.setJobName("WorldUrbanization");
        job.setJarByClass(WorldUrbanization.class);
        job.setMapperClass(WorldUrbanizationMapper.class);
        job.setReducerClass(WorldUrbanizationReducer.class);
        job.setCombinerClass(WorldUrbanizationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class WorldUrbanizationMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            if (key.get() == 0) {
                return;
            } else {
                String line = value.toString();
                String[] parts = line.split(";");

                if (parts[2].equals("1980") && !parts[6].equals("%")) {
                    word.set(parts[0] + " in 1980");
                    context.write(word, new IntWritable(Math.round(Float.valueOf(parts[4]))));
                } else if (parts[2].equals("2006") && !parts[6].equals("%")) {
                    word.set(parts[0] + " in 2006");
                    context.write(word, new IntWritable(Math.round(Float.valueOf(parts[4]))));
                } else if (parts[2].equals("2020") && !parts[6].equals("%")) {
                    word.set(parts[0] + " in 2020");
                    context.write(word, new IntWritable(Math.round(Float.valueOf(parts[4]))));
                } else if (parts[2].equals("2050") && !parts[6].equals("%")) {
                    word.set(parts[0] + " in 2050");
                    context.write(word, new IntWritable(Math.round(Float.valueOf(parts[4]))));
                }
            }
        }
    }

    public static class WorldUrbanizationReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private Map<Text, IntWritable> countMap = new HashMap<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            // We just put the key value into the map
            countMap.put(new Text(key), new IntWritable(sum));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            //Map<Text, IntWritable> sortedMap = sortByValue(countMap);
            Map<Text, IntWritable> sorteMapByKey = new TreeMap<>(countMap);


            for (Text key : sorteMapByKey.keySet()) {
                context.write(key, sorteMapByKey.get(key));
            }
        }

        // This sorts a map by its value (from higher to lower)
        private static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
            List<Map.Entry<K, V>> list = new LinkedList<>(map.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
                @Override
                public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                    return (o2.getValue()).compareTo(o1.getValue());
                }
            });

            Map<K, V> result = new LinkedHashMap<>();
            for (Map.Entry<K, V> entry : list) {
                result.put(entry.getKey(), entry.getValue());
            }
            return result;

        }
    }

}
