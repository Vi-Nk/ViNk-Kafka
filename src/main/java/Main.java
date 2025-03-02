import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
  private static final int THREAD_POOL_SIZE = 4;
  public static void main(String[] args) {
    ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    ServerSocket serverSocket = null;
    int port = 9092;
    System.out.println("Starting Kafka broker at port : " +port);
    try {
      serverSocket = new ServerSocket(port);
      serverSocket.setReuseAddress(true);
      // Wait for connection from client.
      while (true) {
        try {
          Socket clientSocket = serverSocket.accept();
          executorService.submit(new KafkaClientHandler(clientSocket));
        } catch (IOException e) {
          System.out.println("IOException: " + e.getMessage());
        } 

      }

    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    }
  }
}
