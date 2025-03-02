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

    private enum ReqKey {
        PRODUCE((short) 0), FETCH((short) 1), HEARTBEAT((short) 12), API_VERSIONS((short) 18);

        public final short type;

        ReqKey(short type) {
            this.type = type;
        }

        static ReqKey fromValue(short type) {
            return switch (type) {
            case 0 -> PRODUCE;
            case 1 -> FETCH;
            case 12 -> HEARTBEAT;
            case 18 -> API_VERSIONS;
            default -> throw new IllegalStateException("Unexpected value: " + type);
            };
        }

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
            InputMessage msg = new InputMessage(wrap.getInt(), wrap.getShort(4), wrap.getShort(6), wrap.getInt(8));
            System.out.println(msg.toString());
            ReqKey reqKey = ReqKey.fromValue(msg.request_api_key());
            ByteBuffer resp = null;
            switch (reqKey) {
            case PRODUCE:
            case FETCH:
            case HEARTBEAT:
                break;
            case API_VERSIONS:
                resp = handleApiversion(wrap, msg);

            }
            if (resp != null) {
                resp.flip();
                oStream.write(resp.compact().array());
            }

        } catch (Exception e) {
            System.out.println("Exception: " + e.getMessage());
        }
    }

    private ByteBuffer handleApiversion(ByteBuffer inBuffer, InputMessage msg) {
        ByteBuffer response;
        ByteBuffer message = ByteBuffer.allocate(1024);
        message.order(ByteOrder.BIG_ENDIAN);
        message.putInt(msg.correlation_id());
        if (msg.request_api_version() > 4 || msg.request_api_version() < 0) {
            message.putShort((short) 35);
        } else {
            // ApiVersions Response (Version: CodeCrafters) => error_code num_of_api_keys
            // [api_keys] throttle_time_ms TAG_BUFFER
            // error_code => INT16
            // num_of_api_keys => INT8
            // api_keys => api_key min_version max_version
            // api_key => INT16
            // min_version => INT16
            // max_version => INT16
            // _tagged_fields 0
            // throttle_time_ms => INT32
            // _tagged_fields 0
            message.putShort((short) 0); // err code
            message.put((byte) 2); // num of keys 2 ?
            message.putShort(msg.request_api_key());
            message.putShort((short) 0);
            message.putShort((short) 4);
            message.put((byte) 0); // tagged
            message.putInt(0);// throttle time
            message.put((byte) 0); // tagged
        }
        message.flip();
        byte[] getMessage = new byte[message.limit()];
        message.get(getMessage);
        response = ByteBuffer.allocate(getMessage.length + 4);
        response.putInt(getMessage.length);
        response.put(getMessage);
        return response;

    }
}
