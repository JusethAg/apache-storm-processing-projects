import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import spouts.WordReader;
import trident.SplitFunction;

public class StateTopology {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        final String SPOUT_NAME = "Trident-lines-Spout";
        final String TOPOLOGY_NAME = "Trident-Topology";

        // Topology definition
        TridentTopology topology = new TridentTopology();
        TridentState wordCounts =
                topology.newStream(SPOUT_NAME, new WordReader())
                        .each(new Fields("word"),
                                new SplitFunction(),
                                new Fields("word_split"))
                        .groupBy(new Fields("word_split"))
                        .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields());

        LocalDRPC drpc = new LocalDRPC();

        topology.newDRPCStream("count", drpc)
                .stateQuery(wordCounts, new Fields("args"), new MapGet(), new Fields("count"));

        // Configuration
        Config config = new Config();
        config.setDebug(true);
        config.put("fileToRead", System.getProperty("user.home") + "/Desktop/word-counter/trident-sample.txt");

        // Topology run
        LocalCluster cluster = new LocalCluster();

        try {

            cluster.submitTopology(TOPOLOGY_NAME, config, topology.build());
            Thread.sleep(20000);

            for (String word: new String[]{ "short", "very"}) {
                System.out.println("Result for " + word+ ": " + drpc.execute("count", word));
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            cluster.shutdown();
        }


    }
}
