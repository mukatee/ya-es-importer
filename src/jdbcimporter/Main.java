package jdbcimporter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Properties;

/**
 * @author Teemu Kanstren.
 */
public class Main {
  private static String id = null;
  private static int limit = 0;
  private static String seq = null;
  private static String index = null;
  private static String type = null;
  private static Properties props = null;
  private static Long seqValue = null;
  private static String esTarget = null;
  private static int delay = -1;
  private static String jdbcURL = null;
  private static String jdbcUser = null;
  private static String jdbcPW = null;
  private static String sql = null;

  public static void main(String[] args) throws Exception {
    loadConfig();
    Connection conn = DriverManager.getConnection(jdbcURL, jdbcUser, jdbcPW);
    PreparedStatement ps = conn.prepareStatement(sql);
    while (true) {
      poll(ps);
      Thread.sleep(delay);
    }
  }

  private static void loadConfig() throws Exception {
    String errors = "";
    File file = new File("es_importer.properties");
    if (!file.exists()) {
      errors = "Configuration file 'es_importer.properties' not found.";
      throw new RuntimeException(errors);
    }
    String lf = System.getProperty("line.separator");
    props = new Properties();
    props.load(new FileInputStream(file));
    String esURL = props.getProperty("es_url");
    if (esURL == null) errors += "Required configuration property 'es_url' not found."+lf;
    String esPort = props.getProperty("es_port");
    if (esPort == null) errors += "Required configuration property 'es_port' not found."+lf;
    esTarget = "http://"+esURL+":"+esPort+"/_bulk";
    index = props.getProperty("index");
    type = props.getProperty("type");
    jdbcURL = props.getProperty("jdbc_url");
    if (jdbcURL == null) errors += "Required configuration property 'jdbc_url' not found."+lf;
    jdbcUser = props.getProperty("jdbc_user");
    if (jdbcUser == null) errors += "Required configuration property 'jdbc_user' not found."+lf;
    jdbcPW = props.getProperty("jdbc_pw");
    if (jdbcPW == null) errors += "Required configuration property 'jdbc_pw' not found."+lf;
    sql = props.getProperty("sql");
    if (sql == null) errors += "Required configuration property 'sql' not found."+lf;
    id = props.getProperty("id");
    String limitStr = props.getProperty("limit");
    if (limitStr == null) errors += "Required configuration property 'limit' not found."+lf;
    else limit = Integer.parseInt(limitStr);
    seq = props.getProperty("seq");
    String seqValueStr = props.getProperty("seq_value");
    if (seqValueStr != null) seqValue = Long.parseLong(seqValueStr);
    String delayStr = props.getProperty("delay");
    if (delayStr == null) errors += "Required configuration property 'delay' not found."+lf;
    else delay = Integer.parseInt(delayStr)*1000;
    if (errors.length() > 0) throw new RuntimeException(errors);
  }

  private static void flush(String msg) throws Exception {
    MsgSender.send(esTarget, msg);
    System.out.println(msg);
    if (seqValue == null) return;
    File f = new File("es_importer.properties");
    props.setProperty("seq_value", ""+seqValue);
    FileOutputStream out = new FileOutputStream(f);
    props.store(out, "");
    out.close();
  }

  private static void poll(PreparedStatement ps) throws Exception {
    if (seqValue != null) ps.setLong(1, seqValue);
    ResultSet rs = ps.executeQuery();
    ResultSetMetaData meta = rs.getMetaData();
    int count = meta.getColumnCount();
    String[] names = new String[count+1];
    int[] types = new int[count+1];
    int idN = -1;
    int seqN = -1;
    for (int i = 1 ; i <= count ; i++) {
      String name = meta.getColumnName(i);
      names[i] = name;
      types[i] = meta.getColumnType(i);
      if (id != null && name.equals(id)) {
        idN = i;
      }
      if (seq != null && name.equals(seq)) {
        seqN = i;
      }
    }
    String msg = "";
    int rows = 0;
    while (rs.next()) {
      String idValue = null;
      String body = "";
      String line = "{";
      for (int i = 1 ; i <= count ; i++) {
        if (i == idN) {
          idValue = rs.getString(i);
//          continue;
        }
        if (i == seqN) {
          seqValue = rs.getLong(i);
        }
        String value = "";
        switch (types[i]) {
          case Types.BIGINT:
          case Types.SMALLINT:
          case Types.INTEGER:
          case Types.TINYINT:
            value += rs.getLong(i);
            break;
          case Types.BOOLEAN:
            value += rs.getBoolean(i);
            break;
          case Types.DECIMAL:
          case Types.DOUBLE:
          case Types.FLOAT:
            value += rs.getDouble(i);
            value = value.replace(',', '.');
            break;
          case Types.LONGNVARCHAR:
          case Types.NVARCHAR:
          case Types.VARCHAR:
            value += "\""+rs.getString(i)+"\"";
            break;
        }
        line += names[i]+":";
        line += value;
        if (i <= (count -1)) {
          line += ", ";
        }
      }
      body += line+"}";
      String head = "{\"index\":{\"_index\":\""+index+"\", \"_type\":\""+type+"\"";
      if (idValue != null) {
        head += ", \"_id\" : \""+idValue+"\"";
      }
      head += "}}";
      msg += head+"\n";
      msg += body+"\n";
      rows++;
      if (rows >= limit) {
        flush(msg);
        rows = 0;
//        Thread.sleep(5000);
        msg = "";
      }
    }
    rs.close();
    if (msg.length() > 0 ) flush(msg);
  }
}
