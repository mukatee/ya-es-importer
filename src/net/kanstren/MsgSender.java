package net.kanstren;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @author Teemu Kanstren.
 */
public class MsgSender {
  public static String send(String to, String msg) throws Exception {
    HttpURLConnection conn = null;

    conn = (HttpURLConnection) new URL(to).openConnection();
    conn.setRequestMethod("POST");

    conn.setRequestProperty("User-Agent", "es-importer");
//    conn.setRequestProperty("Accept-Language", "en-US,en;q=0.5");

    conn.setDoOutput(true);
    OutputStream out = conn.getOutputStream();
    DataOutputStream wr = new DataOutputStream(out);
    wr.writeBytes(msg);
    wr.flush();
    wr.close();

    int responseCode = conn.getResponseCode();
//      log.debug("\nSending 'POST' request to URL : " + to);
//      log.debug("Post data : " + data.toString());
//      log.debug("Response Code : " + responseCode);

    StringBuilder response = new StringBuilder();
    InputStream in = conn.getInputStream();
    BufferedReader bin = new BufferedReader(new InputStreamReader(in));
    String line = "";
    while ((line = bin.readLine()) != null) {
      response.append(line);
    }
    conn.disconnect();
    //print result
    System.out.println("response:" + response.toString());
    return response.toString();
  }
}
