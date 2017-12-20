/**
 * Autonomic Proprietary 1.0
 *
 * TBD
 **/

package io.confluent.connect.elasticsearch;

import io.searchbox.action.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.client.http.JestHttpClient;
import java.io.IOException;

/**
 * This MockJestClient extends the JestHttpClient to
 * simulate an exception when called by the indexExists method.
 * If this class is initialized with a JestClient, the provided instance
 * is used for execution and shutting down.
 */
public class MockJestClient extends JestHttpClient {

  private static boolean flag = false;
  private JestClient client;
  private boolean throwException;

  public MockJestClient(JestClient object, boolean throwException) {
    client = object;
    this.throwException = throwException;
  }

  public void setException(boolean value) {
    this.throwException = value;
  }

  @Override
  public <T extends JestResult> T execute(Action<T> clientRequest) throws IOException {

    boolean isIndexExists = false;

    StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
    StackTraceElement stackTraceElement = stackTraceElements[2]; //Stack depth of calling class.
    if (stackTraceElement.getMethodName().equals("indexExists")) {
      isIndexExists = true;
    }

    flag = !flag;
    if (client == null || (flag && throwException && isIndexExists)) {
      throw new IOException("Test exception");
    }

    return client.execute(clientRequest);
  }

  @Override
  public void shutdownClient() {
    if (client != null) {
      client.shutdownClient();
    } else {
      super.shutdownClient();
    }
  }
}
