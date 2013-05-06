public class ServerAddress {
    private int serverNumber;
    private String serverName;
    private int port;

    public ServerAddress(int serverNumber, String name, int port) {
        this.serverNumber = serverNumber;
        this.serverName = name;
        this.port = port;
    }

    public String getServerName() {
        return this.serverName;
    }

    public int getPort() {
        return this.port;
    }

    public int getServerNumber() {
        return serverNumber;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof ServerAddress)) {
            return false;
        }

        ServerAddress sa = (ServerAddress) obj;
        if (sa.getServerName().equals(this.serverName)
                && (sa.getServerNumber() == this.serverNumber)
                && (sa.getPort() == this.port)) {
            return true;
        }
        return false;
    }
}