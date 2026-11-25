package io.confluent.proddemo;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * Notify the line supervisor / production planner (andon, Teams, SMS).
 *
 * <p>Mock tool: returns a confirmation with no real side effect.
 */
public class NotifySupervisor extends ScalarFunction {
    public String eval(String lineId, String message) {
        return "ANDON-" + Ids.shortId(lineId + message) + ": supervisor of " + lineId
                + " notified: " + message;
    }
}
