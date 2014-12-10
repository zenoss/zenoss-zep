package org.zenoss.zep.index.impl;

public final class EventIndexBackendTask {

    public enum Op {FLUSH, INDEX_EVENT}

    public final Op op;
    public final String uuid;
    public final Long lastSeen;

    private EventIndexBackendTask(Op op, String uuid, Long lastSeen) {
        this.op = op;
        this.uuid = uuid;
        this.lastSeen = lastSeen;
    }

    public static EventIndexBackendTask Flush() {
        return new EventIndexBackendTask(Op.FLUSH, null, null);
    }

    public static EventIndexBackendTask Index(String uuid, Long lastSeen) {
        if (uuid == null) throw new NullPointerException();
        return new EventIndexBackendTask(Op.INDEX_EVENT, uuid, lastSeen);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("op:").append(op);
        if (uuid != null)
            sb.append(",").append("uuid:").append(uuid);
        if (lastSeen != null)
            sb.append(",").append("lastSeen:").append(lastSeen);
        return sb.toString();
    }

    public static EventIndexBackendTask parse(String s) {
        try {
            Op op = null;
            String uuid = null;
            Long lastSeen = null;
            for (String pairString : s.split(",")) {
                String[] pair = pairString.split(":",2);
                if ("op".equals(pair[0]))
                    op = Op.valueOf(pair[1]);
                else if ("uuid".equals(pair[0]))
                    uuid = pair[1];
                else if ("lastSeen".equals(pair[0]))
                    lastSeen = Long.parseLong(pair[1],10);
            }
            return new EventIndexBackendTask(op, uuid, lastSeen);
        } catch (NullPointerException e) {
            throw new IllegalArgumentException("Unparsable task: " + s, e);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unparsable task: " + s, e);
        }
    }
}
