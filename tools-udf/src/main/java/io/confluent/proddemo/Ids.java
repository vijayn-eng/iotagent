package io.confluent.proddemo;

/** Deterministic short reference ids for the mock tool confirmations. */
final class Ids {
    private Ids() {}

    static String shortId(String seed) {
        return Integer.toHexString((seed == null ? 0 : seed.hashCode()) & 0xfffff).toUpperCase();
    }
}
