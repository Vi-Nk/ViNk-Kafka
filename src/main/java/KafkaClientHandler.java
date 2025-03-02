import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class KafkaClientHandler implements Runnable {
    private final Socket clientSocket;

    public KafkaClientHandler(Socket socket) {
        clientSocket = socket;
    }

    @Override
    public void run() {
        while (!clientSocket.isClosed()) {
            handleRequest();
        }

    }

    private void handleRequest() {
        try {
            InputStream iStream = clientSocket.getInputStream();
            OutputStream oStream = clientSocket.getOutputStream();
            byte[] buff = new byte[1024];
            int ret = iStream.read(buff);
            if (ret == 0 ) return;
            else if (ret == -1 ) {
                clientSocket.close();
                return;
            }
            ByteBuffer wrap = ByteBuffer.wrap(buff);
            InputMessage msg = new InputMessage(wrap.getInt(), wrap.getShort(), wrap.getShort(), wrap.getInt());
            System.out.println(msg.toString());
            RequestKey reqKey = RequestKey.fromValue(msg.request_api_key());
            ByteBuffer resp = null;
            MessageHandler messageHandler = new MessageHandler();
            switch (reqKey) {
            case PRODUCE:
            case FETCH:
            case HEARTBEAT:
                break;
            case API_VERSIONS:
                resp = messageHandler.handleApiversion(wrap, msg);
                break;
            case DESCRIBE_TOPIC_PARTITIONS:
                resp = messageHandler.describeTopicPartitions(wrap,msg);
                break;
            }
            if (resp != null) {
                resp.flip();
                oStream.write(resp.compact().array());
            }

        } catch (Exception e) {
            System.out.println("Exception: " + e.getMessage());
        }
    }
}
