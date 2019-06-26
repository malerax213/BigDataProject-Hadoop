import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class EstimatesProjections extends Configured implements Tool {
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0) {
                return;
            } else {
                String[] line = value.toString().split(";");
                if (line[2].matches(".*Mortality.*")) {
                    String Target = "under 5";
                    if (line[2].matches(".*male.*")) {
                        Target = "male";
                    }
                    if (line[2].matches(".*female.*")) {
                        Target = "female";
                    }
                    if (line[2].matches(".*infant.*")) {
                        Target = "infant";
                    }

                    String[] number = line[7].split("\\.");

                    Text key_data = new Text();
                    key_data.set(line[1]+ "; Mortality " + Target);

                    context.write(new Text(key_data), new IntWritable(Integer.parseInt(number[0])));

                }
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int total = 0;
            for (IntWritable val : values) {
                total = total + val.get();
            }
            context.write(new Text(key), new IntWritable(total));
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf);
        job.setJarByClass(EstimatesProjections.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new EstimatesProjections(), args);
        System.exit(exitCode);
    }
}

