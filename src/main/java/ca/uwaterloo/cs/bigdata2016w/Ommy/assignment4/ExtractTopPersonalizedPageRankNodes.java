package ca.uwaterloo.cs.bigdata2016w.Ommy.assignment4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tl.lin.data.pair.PairOfObjectFloat;
import tl.lin.data.queue.TopScoredObjects;

public class ExtractTopPersonalizedPageRankNodes extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(ExtractTopPersonalizedPageRankNodes.class);

    private static class MyMapper extends
            Mapper<IntWritable, PageRankNode, IntWritable, FloatWritable> {
        private List<TopScoredObjects<Integer>> queue = new ArrayList<>();
        private int numSources;

        @Override
        public void setup(Context context) throws IOException {
            int k = context.getConfiguration().getInt("n", 100);
            numSources = context.getConfiguration().getInt("numSources", 1);
            for (int i = 0; i < numSources; i++) {
                queue.add(new TopScoredObjects<Integer>(k));
            }
        }

        @Override
        public void map(IntWritable nid, PageRankNode node, Context context) throws IOException,
                InterruptedException {
            for (int i = 0; i < numSources; i++) {
                queue.get(i).add(node.getNodeId(), node.getPageRank(i));
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            IntWritable key = new IntWritable();
            FloatWritable value = new FloatWritable();

            for (TopScoredObjects tso : queue) {
                for (PairOfObjectFloat<Integer> pair : tso.extractAll()) {
                    key.set(pair.getLeftElement());
                    value.set(pair.getRightElement());
                    context.write(key, value);
                }
            }
        }
    }

    private static class MyReducer extends
            Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {
        private static List<TopScoredObjects<Integer>> queue;
        private static int numSources;

        @Override
        public void setup(Context context) throws IOException {
            int k = context.getConfiguration().getInt("n", 100);
            numSources = context.getConfiguration().getInt("numSources", 1);
            queue = new ArrayList<>();
            for (int i = 0; i < numSources; i++) {
                queue.add(new TopScoredObjects<Integer>(k));
            }
        }

        @Override
        public void reduce(IntWritable nid, Iterable<FloatWritable> iterable, Context context)
                throws IOException {
            Iterator<FloatWritable> iter = iterable.iterator();
            for (int i = 0; i < numSources; i++) {
                queue.get(i).add(nid.get(), iter.next().get());
            }

            // Shouldn't happen. Throw an exception.
            if (iter.hasNext()) {
                throw new RuntimeException();
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            IntWritable key = new IntWritable();
            FloatWritable value = new FloatWritable();
            String[] sources = context.getConfiguration().get("sources").split(",");
            int i = 0;

            for (TopScoredObjects tso : queue) {
                System.out.println("Source: " + sources[i]);
                for (PairOfObjectFloat<Integer> pair : tso.extractAll()) {
                    key.set(pair.getLeftElement());
                    value.set((float) Math.exp(pair.getRightElement()));
                    context.write(key, value);
                    System.out.println(String.format("%.5f %d", value.get(), pair.getLeftElement()));
                }
                i++;
            }
        }
    }

    public ExtractTopPersonalizedPageRankNodes() {
    }

    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String TOP = "top";
    private static final String SOURCES = "sources";

    /**
     * Runs this tool.
     */
    @SuppressWarnings({"static-access"})
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("top n").create(TOP));
        options.addOption(OptionBuilder.withArgName("sources").hasArg()
                .withDescription("list of sources").create(SOURCES));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(TOP)) {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);
        int n = Integer.parseInt(cmdline.getOptionValue(TOP));
        String[] sources = cmdline.getOptionValue(SOURCES).split(",");

        LOG.info("Tool name: " + ExtractTopPersonalizedPageRankNodes.class.getSimpleName());
        LOG.info(" - input: " + inputPath);
        LOG.info(" - output: " + outputPath);
        LOG.info(" - top: " + n);

        Configuration conf = getConf();
        conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
        conf.setInt("n", n);
        conf.setInt("numSources", sources.length);
        conf.set("sources", cmdline.getOptionValue(SOURCES));

        Job job = Job.getInstance(conf);
        job.setJobName(ExtractTopPersonalizedPageRankNodes.class.getName() + ":" + inputPath);
        job.setJarByClass(ExtractTopPersonalizedPageRankNodes.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // Delete the output directory if it exists already.
        FileSystem.get(conf).delete(new Path(outputPath), true);

        job.waitForCompletion(true);

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new ExtractTopPersonalizedPageRankNodes(), args);
        System.exit(res);
    }
}
