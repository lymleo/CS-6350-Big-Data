package cs6350.assn_1b;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.Pattern;

public class PosCount {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
        conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
        conf.set("mapreduce.framework.name", "yarn");
        Job job = Job.getInstance(conf, "Part Of Speech Count");
        job.setJarByClass(PosCount.class);
        job.setMapperClass(PosMapper.class);
        job.setCombinerClass(Combiner.class);
        job.setReducerClass(PosReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class PosMapper extends Mapper<Object, Text, IntWritable, Text> {
        static Pos pos;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            pos = new Pos("/user/yxl160531/mobyposi.txt");
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = Pattern.compile("\\s*\\b\\s*").split(value.toString());
            for (String word : words) {
                if (word.length() > 4) {
                    context.write(new IntWritable(word.length()), new Text(pos.getPos(word) + "," + pos.isPlindromes(word)));
                }
            }
        }
    }

    public static class PosReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            int[] sums = new int[11];
            for (Text value : values) {
                String[] contents = value.toString().split(",");
                for (int i = 0; i < contents.length; i++) {
                    sums[i + 1] += Integer.valueOf(contents[i]);
                }
            }
            for (int i = 1; i < 10; i++) sums[0] += sums[i];
            sb.append("Length:").append(key.get())
                    .append("\nCount of Words:").append(sums[0])
                    .append("\nDistribution of POS: {Noun:").append(sums[1])
                    .append(" Verb:").append(sums[2])
                    .append(" Adjective:").append(sums[3])
                    .append(" Adverb:").append(sums[4])
                    .append(" Conjunction:").append(sums[5])
                    .append(" Pronoun:").append(sums[6])
                    .append(" Preposition:").append(sums[7])
                    .append(" Interjection:").append(sums[8])
                    .append(" Others:").append(sums[9]).append("}")
                    .append("\nNumber of palindromes:").append(sums[10]);

            context.write(key, new Text(sb.toString()));
        }
    }

    public static class Combiner extends Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int[] sums = new int[10];
            StringBuilder result = new StringBuilder();
            for (Text value : values) {
                String[] contents = value.toString().split(",");
                switch (contents[0]) {
                    case "Noun":
                        sums[0]++;
                        break;
                    case "Verb":
                        sums[1]++;
                        break;
                    case "Adjective":
                        sums[2]++;
                        break;
                    case "Adverb":
                        sums[3]++;
                        break;
                    case "Conjunction":
                        sums[4]++;
                        break;
                    case "Pronoun":
                        sums[5]++;
                        break;
                    case "Preposition":
                        sums[6]++;
                        break;
                    case "Interjection":
                        sums[7]++;
                        break;
                    default:
                        sums[8]++;
                }
                if (contents[1].equals("1")) sums[9]++;
            }
            for (int s : sums) {
                result.append(s).append(",");
            }
            String out = result.toString();
            context.write(key, new Text(out.substring(0, out.length() - 1)));
        }
    }
}

