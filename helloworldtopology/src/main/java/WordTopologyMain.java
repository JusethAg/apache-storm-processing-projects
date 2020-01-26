import bolts.WordCounter;
import grouping.AlphaGrouping;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import spouts.WordReader;

public class WordTopologyMain {

    public static void main(String[] args) {

        final String SPOUT_NAME = "word-reader";
        final String BOLT_NAME = "word-counter";

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_NAME, new WordReader());
        builder.setBolt(BOLT_NAME, new WordCounter(), 2)
                //.shuffleGrouping(SPOUT_NAME);
                //.fieldsGrouping(SPOUT_NAME, new Fields("word"));
                //.allGrouping(SPOUT_NAME);
                .customGrouping(SPOUT_NAME, new AlphaGrouping());

        Config config = new Config();
        config.put("fileToRead", System.getProperty("user.home") + "/Desktop/word-counter/sample.txt");
        config.put("dirToWrite", System.getProperty("user.home") + "/Desktop/word-counter/");
        config.setDebug(true);

        // Remember to change the scope to compile for local purposes
        LocalCluster cluster = new LocalCluster();

        try {

            cluster.submitTopology("WordCounter-Topology", config, builder.createTopology());
            Thread.sleep(10000);

        } catch (InterruptedException e) {

            e.printStackTrace();

        } finally {

            cluster.shutdown();

        }

    }
}
