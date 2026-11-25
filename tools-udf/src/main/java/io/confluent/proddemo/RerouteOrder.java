package io.confluent.proddemo;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * Reroute a work order to another production line (MES dispatch-list move).
 *
 * <p>Mock tool: returns an MES-style confirmation with no real side effect. Swap the body
 * for a call to your MES/APS API (e.g. Siemens Opcenter, SAP DM, Plex) in production.
 */
public class RerouteOrder extends ScalarFunction {
    public String eval(String orderId, String targetLine, String reason) {
        return "MES-MOVE-" + Ids.shortId(orderId + targetLine) + ": work order " + orderId
                + " moved to " + targetLine + " dispatch list. Reason: " + reason;
    }
}
