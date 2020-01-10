package spouts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import yahoofinance.YahooFinance;
import yahoofinance.quotes.stock.StockQuote;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Map;

public class StockPriceSpout extends BaseRichSpout {

    private SpoutOutputCollector spoutOutputCollector;
    private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        this.spoutOutputCollector = spoutOutputCollector;

    }

    public void nextTuple() {

        final String MICROSOFT_QUOTE_KEY = "MSFT";

        try {
            StockQuote quote = YahooFinance.get(MICROSOFT_QUOTE_KEY).getQuote();

            BigDecimal price = quote.getPrice();
            BigDecimal prevClose = quote.getPreviousClose();
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());

            spoutOutputCollector.emit(
                    new Values(MICROSOFT_QUOTE_KEY,
                            simpleDateFormat.format(timestamp),
                            price.doubleValue(),
                            prevClose.doubleValue()
                    )
            );

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("company", "timestamp", "price", "prev_close"));

    }
}
