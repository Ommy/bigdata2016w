package ca.uwaterloo.cs.bigdata2016w.Ommy.assignment7;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfWritables;

public class BooleanRetrievalHBase extends Configured implements Tool {
    private MapFile.Reader index;
    private FSDataInputStream collection;
    private Stack<Set<Integer>> stack;

    HTableInterface TABLE;
    public static final String[] FAMILIES = { "p" };
    public static final byte[] CF = FAMILIES[0].getBytes();

    private BooleanRetrievalHBase() {}

    private void initialize(String indexPath, String collectionPath, FileSystem fs) throws IOException {
        index = new MapFile.Reader(new Path(indexPath + "/part-r-00000"), fs.getConf());
        collection = fs.open(new Path(collectionPath));
        stack = new Stack<Set<Integer>>();
    }

    private void runQuery(String q) throws IOException {
        String[] terms = q.split("\\s+");

        for (String t : terms) {
            if (t.equals("AND")) {
                performAND();
            } else if (t.equals("OR")) {
                performOR();
            } else {
                pushTerm(t);
            }
        }

        Set<Integer> set = stack.pop();

        for (Integer i : set) {
            String line = fetchLine(i);
            System.out.println(i + "\t" + line);
        }
    }

    private void pushTerm(String term) throws IOException {
        stack.push(fetchDocumentSet(term));
    }

    private void performAND() {
        Set<Integer> s1 = stack.pop();
        Set<Integer> s2 = stack.pop();

        Set<Integer> sn = new TreeSet<Integer>();

        for (int n : s1) {
            if (s2.contains(n)) {
                sn.add(n);
            }
        }

        stack.push(sn);
    }

    private void performOR() {
        Set<Integer> s1 = stack.pop();
        Set<Integer> s2 = stack.pop();

        Set<Integer> sn = new TreeSet<Integer>();

        for (int n : s1) {
            sn.add(n);
        }

        for (int n : s2) {
            sn.add(n);
        }

        stack.push(sn);
    }

    private Set<Integer> fetchDocumentSet(String term) throws IOException {
        Set<Integer> set = new TreeSet<Integer>();

        for (PairOfInts pair : fetchPostings(term)) {
            set.add(pair.getLeftElement());
        }

        return set;
    }

    private ArrayListWritable<PairOfInts> fetchPostings(String term) throws IOException {
//        Text key = new Text();
//        PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>> value =
//                new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>();
        Get get = new Get(Bytes.toBytes(term));
        Result result = TABLE.get(get);
        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(CF);
        ArrayListWritable<PairOfInts> pairs = new ArrayListWritable<>();

        for (Map.Entry<byte[], byte[]> entry: familyMap.entrySet()) {
            pairs.add(new PairOfInts(Bytes.toInt(entry.getKey()), Bytes.toInt(entry.getValue())));
        }
//        key.set(term);
//        index.get(key, value);

        return pairs;
    }

    public String fetchLine(long offset) throws IOException {
        collection.seek(offset);
        BufferedReader reader = new BufferedReader(new InputStreamReader(collection));

        String d = reader.readLine();
        return d.length() > 80 ? d.substring(0, 80) + "..." : d;
    }

    public static class Args {
        @Option(name = "-index", metaVar = "[path]", required = true, usage = "index path")
        public String index;

        @Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection path")
        public String collection;

        @Option(name = "-query", metaVar = "[term]", required = true, usage = "query")
        public String query;

        @Option(name = "-table", metaVar = "[name]", required = true, usage = "HBase table to store output")
        public String table;
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

        if (args.collection.endsWith(".gz")) {
            System.out.println("gzipped collection is not seekable: use compressed version!");
            return -1;
        }

        FileSystem fs = FileSystem.get(new Configuration());

        Configuration conf = getConf();

        Configuration hbaseConfig = HBaseConfiguration.create(conf);
        HConnection hbaseConnection = HConnectionManager.createConnection(hbaseConfig);
        TABLE = hbaseConnection.getTable(args.table);

        initialize(args.index, args.collection, fs);

        System.out.println("Query: " + args.query);
        long startTime = System.currentTimeMillis();
        runQuery(args.query);
        System.out.println("\nquery completed in " + (System.currentTimeMillis() - startTime) + "ms");

        return 1;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new BooleanRetrievalHBase(), args);
    }
}
