import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
  private static final int THREAD_POOL_SIZE = 4;
  public static void main(String[] args) {
    // You can use print statements as follows for debugging, they'll be visible
    // when running tests.
    System.err.println("Logs from your program will appear here!");
    ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    // Uncomment this block to pass the first stage
    //
    ServerSocket serverSocket = null;
    int port = 9092;
    try {
      serverSocket = new ServerSocket(port);
      // Since the tester restarts your program quite often, setting SO_REUSEADDR
      // ensures that we don't run into 'Address already in use' errors
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
