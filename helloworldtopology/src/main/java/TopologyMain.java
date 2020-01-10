import bolts.StockPriceBoult;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import spouts.StockPriceSpout;

public class TopologyMain {

    public static void main(String[] args) {

        final String SPOUT_NAME = "Stock-Price-Spout";
        final String BOLT_NAME = "Stock-Price-Bolt";

        // Build topology
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout(SPOUT_NAME, new StockPriceSpout());
        topologyBuilder.setBolt(BOLT_NAME, new StockPriceBoult())
                .shuffleGrouping(SPOUT_NAME);

        StormTopology stormTopology = topologyBuilder.createTopology();

        // Config topology
        Config config = new Config();
        config.setDebug(true);
        config.put("fileToWrite", System.getProperty("user.home") + "/Desktop/HelloStorm/StockPrices.txt");

        //Submit topology to cluster
        LocalCluster localCluster = new LocalCluster();


        try {
            localCluster.submitTopology("Stock-Trader-Topology", config, stormTopology);
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            localCluster.shutdown();
        }

    }
}
