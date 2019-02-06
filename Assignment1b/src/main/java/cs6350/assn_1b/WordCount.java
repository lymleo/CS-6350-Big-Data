package cs6350.assn_1b;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class WordCount {
	private static final Logger LOG = Logger.getLogger(WordCount.class);

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private static Map<Text, Text> dictionary = new HashMap<>();

		protected void setup(Mapper.Context context) throws IOException {
			// read positive and negative words and save into a hashmap Dictionary;

			String posFile = "/user/yxl160531/opinion-lexicon-English/positive-words.txt";
			Path posPt = new Path(posFile);
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(posPt)));
			String str;
			while ((str = bufferedReader.readLine()) != null)
				dictionary.put(new Text(str), new Text("pos"));

			String negFile = "/user/yxl160531/opinion-lexicon-English/negative-words.txt";
			Path negPt = new Path(negFile);
			bufferedReader = new BufferedReader(new InputStreamReader(fs.open(negPt)));
			while ((str = bufferedReader.readLine()) != null)
				dictionary.put(new Text(str), new Text("neg"));
			bufferedReader.close();
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] words = Pattern.compile("\\s*\\b\\s*").split(value.toString());
			int posCount = 0;
			int negCount = 0;
			for (String w : words) {
				if (dictionary.get(new Text(w.toLowerCase())) != null
						&& dictionary.get(new Text(w.toLowerCase())).equals(new Text("pos")))
					posCount++;
				if (dictionary.get(new Text(w.toLowerCase())) != null
						&& dictionary.get(new Text(w.toLowerCase())).equals(new Text("neg")))
					negCount++;
			}
			context.write(new Text("pos"), new IntWritable(posCount));
			context.write(new Text("neg"), new IntWritable(negCount));
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	// type input path in args[0] and output path in args[1];
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
		conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
		conf.set("mapreduce.framework.name", "yarn");
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
