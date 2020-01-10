package bolts;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Map;

public class StockPriceBoult extends BaseBasicBolt {

    private PrintWriter printWriter;


    public void prepare(Map stormConf, TopologyContext context) {

        String fileName = stormConf.get("fileToWrite").toString();

        try {
            this.printWriter = new PrintWriter(fileName, "UTF-8");
        } catch (Exception e) {
            throw new RuntimeException("Error opening file [" + fileName + "]");
        }

    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        // Four different ways to get values from tuple
        String quoteKey = tuple.getValue(0).toString();
        String timestamp = tuple.getString(1);
        Double price = (Double) tuple.getValueByField("price");
        Double prevClose = tuple.getDoubleByField("prev_close");

        Boolean gain = !(price <= prevClose);

        // Since we aren't going to use an another source, this emit isn't necessary
        basicOutputCollector.emit(new Values(quoteKey, timestamp, price, gain));
        printWriter.println(quoteKey + ", " + timestamp + ", " + price + ", " + gain);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("company", "timestamp", "price", "gain"));

    }


    public void cleanup() {
        printWriter.close();
    }
}
