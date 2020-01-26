import bolts.RandomFailureBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.tuple.Fields;
import spouts.ReliableWordReader;
import spouts.WordReader;
import trident.SplitFunction;

public class TridentTopologyMain {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        final String SPOUT_NAME = "Trident-lines-Spout";
        final String TOPOLOGY_NAME = "Trident-Topology";

        // Topology definition
        TridentTopology topology = new TridentTopology();
        topology.newStream(SPOUT_NAME, new WordReader())
                .each(new Fields("word"),
                        new SplitFunction(),
                        new Fields("word_split"))
                //.each(new Fields("word_split"), new Debug());  //Split input
                // .aggregate(new Count(), new Fields("count")) // Count
                // .each(new Fields("count"), new Debug());
                .groupBy(new Fields("word_split"))// Group by word
                .aggregate(new Count(), new Fields("count"))
                .each(new Fields("word_split", "count"), new Debug());

        // Configuration
        Config config = new Config();
        config.setDebug(true);
        config.put("fileToRead", System.getProperty("user.home") + "/Desktop/word-counter/trident-sample.txt");

        // Topology run

        if (args.length != 0 && args[0].equals("remote")) {
            StormSubmitter.submitTopology(TOPOLOGY_NAME, config, topology.build());
        } else {

            LocalCluster cluster = new LocalCluster();

            try {

                cluster.submitTopology(TOPOLOGY_NAME, config, topology.build());
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                cluster.shutdown();
            }

        }

    }
}
