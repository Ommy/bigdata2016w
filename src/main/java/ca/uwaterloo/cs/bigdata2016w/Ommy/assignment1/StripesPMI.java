package ca.uwaterloo.cs.bigdata2016w.Ommy.assignment1;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
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

import tl.lin.data.map.HMapStFW;
import tl.lin.data.pair.PairOfStrings;


/**
 * Created by fasihawan on 2016-01-12.
 */
public class StripesPMI  extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(StripesPMI.class);
    private static final String TEMP_OUTPUT_PATH = "temp_path";
    private static final String LINE_COUNTER = "line_counter";
    private static final String WORD_COUNT_PATH = "TEMP_OUTPUT_PATH";

    enum LineCounter {NumberOfLinesCounter}

    public StripesPMI() {}

    protected static class StripesPartitioner extends Partitioner<PairOfStrings, IntWritable> {
        @Override
        public int getPartition(PairOfStrings key, IntWritable value, int numReduceTasks) {
            return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    private static class StripesMapper extends Mapper<LongWritable, Text, Text, HMapStFW> {

        private final static Text TEXT = new Text();

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

            Map<String, HMapStFW> stripes = new HashMap<>();
            HMapStFW stripe;
            for (int i = 0; i < words.length; i++) {
                if (stripes.containsKey(words[i])) {
                    stripe = stripes.get(words[i]);
                } else {
                    stripe = new HMapStFW();
                }
                for (int j = 0; j < words.length; j++) {
                    if (i == j) continue;
                    if (stripe.containsKey(words[j])) {
                        stripe.increment(words[j], 1f);
                    } else {
                        stripe.put(words[j], 1f);
                    }
                }
                stripes.put(words[i], stripe);
            }

            for (String txt: stripes.keySet()) {
                TEXT.set(txt);
                context.write(TEXT, stripes.get(txt));
            }
        }

    }

    private static class StripesCombiner extends Reducer<Text, HMapStFW, Text, HMapStFW> {
        private final static IntWritable SUM = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<HMapStFW> values, Context context)
                throws IOException, InterruptedException {
            Iterator<HMapStFW> iter = values.iterator();
            HMapStFW sums = new HMapStFW();
            while (iter.hasNext()) {
                HMapStFW temp = iter.next();
                for (String txt: temp.keySet()) {
                    if (sums.containsKey(txt)) {
                        sums.increment(txt, temp.get(txt));
                    } else {
                        sums.put(txt, temp.get(txt));
                    }
                }
            }
            context.write(key, sums);
        }
    }

    private static class StripesReducer extends Reducer<Text, HMapStFW, Text, Text> {

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
        protected void reduce(Text key, Iterable<HMapStFW> values, Context context)
                throws IOException, InterruptedException {

            Long N = Long.parseLong(context.getConfiguration().get(LINE_COUNTER));
            int keyStringCount = wordCountMap.get(key.toString());

            Iterator<HMapStFW> iter = values.iterator();
            HMapStFW sums = new HMapStFW();
            while (iter.hasNext()) {
                HMapStFW temp = iter.next();
                for (String txt: temp.keySet()) {
                    if (sums.containsKey(txt)) {
                        sums.increment(txt, temp.get(txt));
                    } else {
                        sums.put(txt, temp.get(txt));
                    }
                }
            }

            for (String txt: sums.keySet()) {
                int tempStringCount = wordCountMap.get(txt);
                float sum = sums.get(txt);
                if (Float.compare(sum, 10f) < 0) continue;
                double numerator = N * sum;
                double denominator = (keyStringCount) * (tempStringCount);
                double PMIResult;
                if (Double.compare(denominator, 0.0) == 0) {
                    PMIResult = 0.0;
                } else {
                    PMIResult = Math.log10(numerator / denominator);
                }
                PMI.set(PMIResult+"");
                PAIR.set((new PairOfStrings(key.toString(), txt)).toString());
                context.write(PAIR, PMI);
            }
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

        LOG.info("Tool: " + StripesPMI.class.getSimpleName());
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
        job1.setJobName(StripesPMI.class.getSimpleName());
        job1.setJarByClass(StripesPMI.class);

        job1.setNumReduceTasks(args.numReducers);

        FileInputFormat.setInputPaths(job1, new Path(args.input));
        TextOutputFormat.setOutputPath(job1, new Path(TEMP_OUTPUT_PATH));

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        job1.setMapperClass(args.imc ? WordCountMapper.class : WordCountMapper.class);
        job1.setCombinerClass(WordCountCombiner.class);
        job1.setReducerClass(WordCountReducer.class);

        /**
         *  Job 2
         *
         *  Stripes PMI Job
         */

        job1.waitForCompletion(true);
        Long counter = job1.getCounters().findCounter(LineCounter.NumberOfLinesCounter).getValue();
        conf.set(LINE_COUNTER, counter.toString());
        conf.set(WORD_COUNT_PATH, TEMP_OUTPUT_PATH);

        Job job2 = Job.getInstance(conf, "Job 2");
        job2.setJobName(StripesPMI.class.getSimpleName());
        job2.setJarByClass(StripesPMI.class);

        job2.setNumReduceTasks(args.numReducers);

        FileInputFormat.setInputPaths(job2, new Path(args.input));
        TextOutputFormat.setOutputPath(job2, new Path(args.output));

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(HMapStFW.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        job2.setMapperClass(StripesMapper.class);
//        job2.setPartitionerClass(StripesPartitioner.class);
        job2.setCombinerClass(StripesCombiner.class);
        job2.setReducerClass(StripesReducer.class);

        long startTime = System.currentTimeMillis();
        job1.waitForCompletion(true);
        LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        fs.delete(new Path(args.output), true);
        job2.waitForCompletion(true);
        fs.delete(new Path(TEMP_OUTPUT_PATH), true);
        return 0;
    }

    public static void main(String[] args) throws Exception{
        ToolRunner.run(new StripesPMI(), args);
    }
}
