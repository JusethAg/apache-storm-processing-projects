package bolts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class RandomFailureBolt extends BaseRichBolt {

    private static final Integer MAX_PERCENT_FAIL = 8;
    private Random random = new Random();
    private OutputCollector collector;


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    public void execute(Tuple tuple) {

        Integer r = random.nextInt(10);

        if (r < MAX_PERCENT_FAIL) {

            collector.emit(tuple, new Values(tuple.getString(0)));
            collector.ack(tuple);

        } else {

            collector.fail(tuple);

        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
