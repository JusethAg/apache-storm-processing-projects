import bolts.RandomFailureBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import spouts.ReliableWordReader;

public class ReliableTopologyMain {

    public static void main(String[] args) {

        final String SPOUT_NAME = "Reliable-Spout";
        final String BOLT_NAME = "Random reliable-Bolt";

        // Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_NAME, new ReliableWordReader());
        builder.setBolt(BOLT_NAME, new RandomFailureBolt())
                .shuffleGrouping(SPOUT_NAME);

        // Configuration
        Config config = new Config();
        config.setDebug(true);
        config.put("fileToRead", System.getProperty("user.home") + "/Desktop/word-counter/sample.txt");

        // Topology run
        LocalCluster cluster = new LocalCluster();

        try {

            cluster.submitTopology("Random-Fail-Topology", config, builder.createTopology());
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            cluster.shutdown();
        }
    }
}
