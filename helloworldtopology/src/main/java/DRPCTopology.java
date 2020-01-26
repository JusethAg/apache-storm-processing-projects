import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.tuple.Fields;
import spouts.WordReader;
import trident.SplitFunction;

public class DRPCTopology {

    public static void main(String[] args) throws Exception {

        final String DRPC_NAME = "DRPC-lines-Spout";
        final String TOPOLOGY_NAME = "DRPC-Topology";

        LocalDRPC drpc = new LocalDRPC();

        // Topology definition
        TridentTopology topology = new TridentTopology();
        topology.newDRPCStream(DRPC_NAME, drpc)
                .each(new Fields("args"),
                        new SplitFunction(),
                        new Fields("word_split"))
                .groupBy(new Fields("word_split"))
                .aggregate(new Count(), new Fields("count"));

        // Configuration
        Config config = new Config();
        config.setDebug(true);

        // Topology run
        LocalCluster cluster = new LocalCluster();

        try {

            cluster.submitTopology(TOPOLOGY_NAME, config, topology.build());

            for (String word : new String[]{"a random text line", "another text line"}) {

                System.out.println("Result for" + word + ": " + drpc.execute("split", word));
            }


        } finally {
            cluster.shutdown();
        }

    }
}