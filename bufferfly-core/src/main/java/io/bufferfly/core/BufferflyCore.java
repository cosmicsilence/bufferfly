package io.bufferfly.core;

/**
 * Entry point / marker class for the bufferfly-core module.
 */
public final class BufferflyCore {

    private BufferflyCore() {}

    public static String version() {
        String version = BufferflyCore.class.getPackage().getImplementationVersion();
        return version != null ? version : "unknown";
    }
}
