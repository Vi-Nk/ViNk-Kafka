import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class MessageHandler {

        private static byte TAGGED_BUFFER = (byte)0;
        private static int THROTTLE_TIME = 0;
        private static short UNKNOWN_TOPIC = 3;
        private byte[] DEFAULT_UUID = new byte[16];

        public ByteBuffer handleApiversion(ByteBuffer inBuffer, InputMessage msg) {
        ByteBuffer response;
        ByteBuffer message = ByteBuffer.allocate(1024);
        message.order(ByteOrder.BIG_ENDIAN);
        message.putInt(msg.correlation_id());
        if (msg.request_api_version() > 4 || msg.request_api_version() < 0) {
            message.putShort((short) 35);
        } else {
            // ApiVersions Response (Version: 4) => error_code num_of_api_keys
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
            message.put((byte) 3); // num of keys 2 ?
            message.putShort(msg.request_api_key());
            message.putShort((short) 0);
            message.putShort((short) 4);
            message.put(TAGGED_BUFFER); // tagged
            message.putShort((short)75); // DescribeTopicPartitions
            message.putShort((short) 0); // min
            message.putShort((short) 0); // max
            message.put(TAGGED_BUFFER);  //tagged  
            message.putInt(THROTTLE_TIME);// throttle time
            message.put(TAGGED_BUFFER); // tagged
        }
        message.flip();
        byte[] getMessage = new byte[message.limit()];
        message.get(getMessage);
        response = ByteBuffer.allocate(getMessage.length + 4);
        response.putInt(getMessage.length);
        response.put(getMessage);
        return response;

    }

    public ByteBuffer describeTopicPartitions(ByteBuffer inBuffer, InputMessage msg)
    {
        ByteBuffer response = null;
        ByteBuffer message = ByteBuffer.allocate(1024);
        message.order(ByteOrder.BIG_ENDIAN);
        message.putInt(msg.correlation_id());
        if (msg.request_api_version() != 0)
        {
            message.putShort((short) 35);
        }
        else {
            message.put(TAGGED_BUFFER);
            message.putInt(THROTTLE_TIME);
            short skip = inBuffer.getShort();
            byte[] clientID = new byte[skip+1];
            inBuffer.get(clientID);
            byte topiclen = inBuffer.get();
            byte[][] topics = new byte[topiclen-1][255];
            for ( int i = 0 ; i < topiclen-1 ; i ++)
            {
                byte topicNameLen = inBuffer.get();
                topics[i] = new byte[topicNameLen-1];
                inBuffer.get(topics[i]);
            }

            //return unknown buffer as of now 
            message.put(topiclen);
            message.putShort(UNKNOWN_TOPIC); //error code 3 unknown topic
            for (int i=0 ; i < topiclen-1 ; i ++){
                message.put((byte)(topics[i].length+1));
                message.put(topics[i]);
                message.put(DEFAULT_UUID);
                message.put((byte)0); //false for internal topic
            }
            message.put((byte)1); // compact array size = 0
            message.put(new byte[] {0 , 0 , (byte)13 ,(byte)248}); // topic auth op 
            message.put(TAGGED_BUFFER);
            message.put((byte)255); //next cursor
            message.put(TAGGED_BUFFER);

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
