package ca.uwaterloo.cs.bigdata2016w.Ommy.assignment1;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import tl.lin.data.pair.PairOfStrings;


/**
 * Created by fasihawan on 2016-01-12.
 */
public class PairsPMI  extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(PairsPMI.class);
    private static final String TEMP_OUTPUT_PATH = "temp_path";
    private static final String LINE_COUNTER = "line_counter";
    private static final String WORD_COUNT_PATH = "TEMP_OUTPUT_PATH";

    enum LineCounter {NumberOfLinesCounter}

    public PairsPMI() {}

    protected static class PairsPartitioner extends Partitioner<PairOfStrings, IntWritable> {
        @Override
        public int getPartition(PairOfStrings key, IntWritable value, int numReduceTasks) {
            return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    private static class PairsMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {

        private final static PairOfStrings PAIR = new PairOfStrings();
        private final static IntWritable INT = new IntWritable();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = ((Text) value).toString();
            StringTokenizer itr = new StringTokenizer(line);

            int cnt = 0;
            Set set = Sets.newHashSet();
            while (itr.hasMoreTokens()) {
                cnt++;
                String w = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
                if (w.length() == 0) continue;
                set.add(w);
                if (cnt >= 100) break;
            }

            String[] words = new String[set.size()];
            words = (String[])set.toArray(words);

            Set<PairOfStrings> cooccurence = new HashSet<>();
            if (words.length < 2) return;
            for (int i = 0; i < words.length; i++) {
                for (int j = 0; j < words.length; j++) {
                    if (i == j) continue;
                    if (words[i].compareTo(words[j]) == 0) continue;
                    cooccurence.add(new PairOfStrings(words[i], words[j]));
                }
            }

            for (PairOfStrings pos: cooccurence) {
                INT.set(1);
                PAIR.set(pos.getLeftElement(), pos.getRightElement());
                context.write(PAIR, INT);
            }
        }

    }

    private static class PairsCombiner extends Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
        private final static IntWritable SUM = new IntWritable();

        @Override
        public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            Iterator<IntWritable> iter = values.iterator();
            int sum = 0;
            while (iter.hasNext()) {
                sum += iter.next().get();
            }
            SUM.set(sum);
            context.write(key, SUM);
        }
    }

    private static class PairsReducer extends Reducer<PairOfStrings, IntWritable, Text, Text> {

        private static final Map<String, Integer> wordCountMap = new HashMap<>();
        private static final Text PAIR = new Text();
        private static final Text PMI = new Text();

        protected void loadMap(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] fss = fs.listStatus(new Path(conf.get(WORD_COUNT_PATH)));
            for (FileStatus status : fss) {
                Path path = status.getPath();
                if (path.getName().contains("SUCCESS")) continue;
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(path)));

                String line = bufferedReader.readLine();
                while (line != null) {
                    String[] count = line.split("\t");
                    wordCountMap.put(count[0], Integer.parseInt(count[1]));
                    line = bufferedReader.readLine();
                }
                bufferedReader.close();
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            loadMap(context);
        }

        @Override
        protected void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            Long N = Long.parseLong(context.getConfiguration().get(LINE_COUNTER));
            int leftStringCount = wordCountMap.get(key.getLeftElement());
            int rightStringCount = wordCountMap.get(key.getRightElement());

            int sum = 0;
            Iterator<IntWritable> iter = values.iterator();

            while (iter.hasNext()) {
                sum += iter.next().get();
            }
            if (sum < 10) return;
            double numerator = N * sum;
            double denominator = (leftStringCount) * (rightStringCount);
            double PMIResult;
            if (Double.compare(denominator, 0.0) == 0) {
                PMIResult = 0.0;
            } else {
                PMIResult = Math.log10(numerator / denominator);
            }

            PMI.set(PMIResult+"");
            PAIR.set(key.toString());
            context.write(PAIR, PMI);

        }
    }

    private static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static Text TEXT = new Text();
        private final static IntWritable INT = new IntWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = ((Text) value).toString();
            StringTokenizer itr = new StringTokenizer(line);

            int cnt = 0;
            Set set = Sets.newHashSet();
            while (itr.hasMoreTokens()) {
                cnt++;
                String w = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
                if (w.length() == 0) continue;
                set.add(w);
                if (cnt >= 100) break;
            }

            String[] words = new String[set.size()];
            words = (String[])set.toArray(words);

            context.getCounter(LineCounter.NumberOfLinesCounter).increment(1L);
            for (String word: words) {
                TEXT.set(word);
                INT.set(1);
                context.write(TEXT, INT);
            }


        }

    }

    private static class WordCountReducer extends Reducer<Text, IntWritable, Text, Text> {
        private final static Text WORD = new Text();
        private final static Text COUNT = new Text();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            Iterator<IntWritable> iter = values.iterator();
            int sum = 0;

            while (iter.hasNext()) {
                sum += iter.next().get();
            }
            WORD.set(key);
            COUNT.set(sum + "");

            context.write(WORD, COUNT);
        }
    }

    private static class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final static IntWritable SUM = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            // Sum up values.
            Iterator<IntWritable> iter = values.iterator();
            int sum = 0;
            while (iter.hasNext()) {
                sum += iter.next().get();
            }
            SUM.set(sum);
            context.write(key, SUM);
        }
    }


    public static class Args {
        @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
        public String input;

        @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
        public String output;

        @Option(name = "-reducers", metaVar = "[num]", required = false, usage = "number of reducers")
        public int numReducers = 1;

        @Option(name = "-imc", usage = "use in-mapper combining")
        boolean imc = false;
    }

    /**
     * Runs this tool.
     */
    public int run(String[] argv) throws Exception {
        Args args = new Args();
        CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

        try {
            parser.parseArgument(argv);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            return -1;
        }

        LOG.info("Tool: " + PairsPMI.class.getSimpleName());
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + args.output);
        LOG.info(" - number of reducers: " + args.numReducers);
        LOG.info(" - use in-mapper combining: " + args.imc);

        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(TEMP_OUTPUT_PATH), true);

        /**
         * Job 1
         *
         * Word Count Job
         */
        Job job1 = Job.getInstance(conf, "Job 1");
        job1.setJobName(PairsPMI.class.getSimpleName());
        job1.setJarByClass(PairsPMI.class);

        job1.setNumReduceTasks(args.numReducers);

        FileInputFormat.setInputPaths(job1, new Path(args.input));
        TextOutputFormat.setOutputPath(job1, new Path(TEMP_OUTPUT_PATH));

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        job1.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 64);
        job1.getConfiguration().set("mapreduce.map.memory.mb", "3072");
        job1.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
        job1.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
        job1.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

        job1.setMapperClass(args.imc ? WordCountMapper.class : WordCountMapper.class);
        job1.setCombinerClass(WordCountCombiner.class);
        job1.setReducerClass(WordCountReducer.class);

        long startTime = System.currentTimeMillis();
        job1.waitForCompletion(true);

        Long counter = job1.getCounters().findCounter(LineCounter.NumberOfLinesCounter).getValue();
        conf.set(LINE_COUNTER, counter.toString());
        conf.set(WORD_COUNT_PATH, TEMP_OUTPUT_PATH);

        /**
         *  Job 2
         *
         *  Pairs PMI Job
         */

        Job job2 = Job.getInstance(conf, "Job 2");
        job2.setJobName(PairsPMI.class.getSimpleName());
        job2.setJarByClass(PairsPMI.class);

        job2.setNumReduceTasks(args.numReducers);

        FileInputFormat.setInputPaths(job2, new Path(args.input));
        TextOutputFormat.setOutputPath(job2, new Path(args.output));

        job2.setMapOutputKeyClass(PairOfStrings.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        job2.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 64);
        job2.getConfiguration().set("mapreduce.map.memory.mb", "3072");
        job2.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
        job2.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
        job2.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

        job2.setMapperClass(PairsMapper.class);
        job2.setPartitionerClass(PairsPartitioner.class);
        job2.setCombinerClass(PairsCombiner.class);
        job2.setReducerClass(PairsReducer.class);

        fs.delete(new Path(args.output), true);
        job2.waitForCompletion(true);

        LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
        fs.delete(new Path(TEMP_OUTPUT_PATH), true);
        return 0;
    }

    public static void main(String[] args) throws Exception{
        ToolRunner.run(new PairsPMI(), args);
    }
}
