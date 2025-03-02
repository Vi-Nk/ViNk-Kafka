public enum RequestKey {
    PRODUCE((short) 0), 
    FETCH((short) 1), 
    HEARTBEAT((short) 12), 
    API_VERSIONS((short) 18),
    DESCRIBE_TOPIC_PARTITIONS((short)75);

    public final short type;

    RequestKey(short type) {
        this.type = type;
    }

    static RequestKey fromValue(short type) {
        return switch (type) {
        case 0 -> PRODUCE;
        case 1 -> FETCH;
        case 12 -> HEARTBEAT;
        case 18 -> API_VERSIONS;
        case 75 -> DESCRIBE_TOPIC_PARTITIONS;
        default -> throw new IllegalStateException("Unexpected value: " + type);
        };
    }
}
