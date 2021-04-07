package spamdetection.rpcserver.bizimpl.mr;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class GlobalCounters {

    private static Map<String, Counter> counters;
    static {
        counters = Collections.synchronizedMap(
                new HashMap<String, Counter>()
        );
    }

    public static void  setCounterValue(String counterKey, long value) {
        if (!counters.containsKey(counterKey)) {
            Counter counter = new Counter();
            counters.put(counterKey, counter);
        }
        counters.get(counterKey).setValue(value);
    }

    public static long getCounterValue(String counterKey) {
        if (!counters.containsKey(counterKey)) {
            return 0;
        }
        return counters.get(counterKey).getValue();
    }

    public static void increment(String counterKey, long value) {
        if (!counters.containsKey(counterKey)) {
            Counter counter = new Counter();
            counters.put(counterKey, counter);
        }
        counters.get(counterKey).increment(value);

    }



















}
