package trident;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class SplitFunction extends BaseFunction {

    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {

        String sentence = tridentTuple.getString(0);
        String [] words = sentence.split(" ");

        for (String word : words) {
            word = word.trim();
            if (!(word.length() < 1)) {
                tridentCollector.emit(new Values(word));
            }
        }
    }
}
