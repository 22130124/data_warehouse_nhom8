import java.sql.*;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import javax.xml.parsers.*;
import org.w3c.dom.*;

public class LoadToWH {

    public static void main(String[] args) throws Exception {

        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Ho_Chi_Minh"));

        if (args.length < 1) {
            System.out.println("Usage: java LoadToWH <config.xml> [dateParam]");
            return;
        }

        String configFile = args[0];
        String dateParam = (args.length >= 2)
                ? args[1]
                : new SimpleDateFormat("yyyy-MM-dd").format(new java.util.Date());

        // =========================
        // 1. Load file config để lấy thông tin kết nối với db_control, db_staging, db_warehouse đồng thời lấy đường dẫn thư mục nơi xuất file dữ liệu cần chuyển (file dump).
        // =========================
        String controlHost="", controlDB="", controlUser="", controlPass="";
        int controlPort=3306;

        String stagingHost="", stagingDB="", stagingUser="", stagingPass="";
        int stagingPort=3306;

        String warehouseHost="", warehouseDB="", warehouseUser="", warehousePass="";
        int warehousePort=3306;

        String dumpFolder="";
        String loadScriptPath="";
        String sshUser="";
        String targetPath="";

        try {
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(new File(configFile));
            doc.getDocumentElement().normalize();

            // db_control
            Element controlElement = (Element) doc.getElementsByTagName("control").item(0);
            controlHost = controlElement.getElementsByTagName("host").item(0).getTextContent();
            controlPort = Integer.parseInt(controlElement.getElementsByTagName("port").item(0).getTextContent());
            controlDB = controlElement.getElementsByTagName("database").item(0).getTextContent();
            controlUser = controlElement.getElementsByTagName("user").item(0).getTextContent();
            controlPass = controlElement.getElementsByTagName("password").item(0).getTextContent();

            // db_staging
            Element stagingElement = (Element) doc.getElementsByTagName("staging").item(0);
            stagingHost = stagingElement.getElementsByTagName("host").item(0).getTextContent();
            stagingPort = Integer.parseInt(stagingElement.getElementsByTagName("port").item(0).getTextContent());
            stagingDB = stagingElement.getElementsByTagName("database").item(0).getTextContent();
            stagingUser = stagingElement.getElementsByTagName("user").item(0).getTextContent();
            stagingPass = stagingElement.getElementsByTagName("password").item(0).getTextContent();

            // db_warehouse
            Element warehouseElement = (Element) doc.getElementsByTagName("warehouse").item(0);
            warehouseHost = warehouseElement.getElementsByTagName("host").item(0).getTextContent();
            warehousePort = Integer.parseInt(warehouseElement.getElementsByTagName("port").item(0).getTextContent());
            warehouseDB = warehouseElement.getElementsByTagName("database").item(0).getTextContent();
            warehouseUser = warehouseElement.getElementsByTagName("user").item(0).getTextContent();
            warehousePass = warehouseElement.getElementsByTagName("password").item(0).getTextContent();

            // dump folder
            Element loadtowhElement = (Element) doc.getElementsByTagName("loadtowh").item(0);
            dumpFolder = loadtowhElement.getElementsByTagName("dump_path").item(0).getTextContent();
            if (!dumpFolder.startsWith("/")) dumpFolder = "/" + dumpFolder;

            loadScriptPath = "/opt/dw/staging/loadtowh/scripts/load_to_wh_with_retry.sh";

        } catch (Exception e) {
            System.err.println("Khong doc duoc config.xml");
            e.printStackTrace();
            return;
        }

        // =========================
        // Bước 2: Kết nối vào db_control
        // =========================
        Connection controlConn = null;

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            String controlUrl = String.format(
                    "jdbc:mysql://%s:%d/%s?serverTimezone=Asia/Ho_Chi_Minh",
                    controlHost, controlPort, controlDB
            );
            controlConn = DriverManager.getConnection(controlUrl, controlUser, controlPass);

        } catch (Exception e) {
            System.err.println("Khong ket noi duoc db_control");
            e.printStackTrace();
            return;
        }

        long startTime = System.currentTimeMillis();
        String dumpFile = null;

        try {
            // =========================
            // Bước 3:  Đọc bảng load_to_wh_config để lấy thông tin:
            // - Tên procedure kiểm tra dữ liệu
            // - SSH user để scp sang server warehouse
            // - Đường dẫn target: là đường dẫn chứa file dump bên server warehouse
            // =========================
            String procedureName="";

            try (PreparedStatement ps = controlConn.prepareStatement(
                    "SELECT is_process_done_procedure, target_path, sshuser FROM load_to_wh_config LIMIT 1")) {
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    procedureName = rs.getString("is_process_done_procedure");
                    targetPath = rs.getString("target_path");
                    sshUser = rs.getString("sshuser");

                    if (!targetPath.startsWith("/")) targetPath = "/" + targetPath;
                }
            }

            // =========================
            // Bước 4: Gọi procedure is_process_done_procedure để kiểm tra dữ liệu trên db_staging đã có hay chưa và gán vào biến result.
            // =========================
            int result = 0;

            try (CallableStatement cs = controlConn.prepareCall("{CALL is_process_done_procedure(?)}")) {
               cs.setDate(1, java.sql.Date.valueOf(dateParam));
               try (ResultSet rs = cs.executeQuery()) {
                  if (rs.next()) {
                  result = rs.getInt("is_done");
                  }
               }
            } catch (SQLException e) {
              String msg = "Khong the goi procedure"; 
              System.err.println(msg);
              insertLog(controlConn, dateParam, "Failed", 0, startTime, System.currentTimeMillis(), msg);
              return;  
            }

            if (result != 1) {
               String msg = "Chua co du lieu de load vao warehouse";
               insertLog(controlConn, dateParam, "Failed", 0, startTime, System.currentTimeMillis(), msg);
               return;
            }

            // =========================
            // Bước 5: Dump dữ liệu từ table staging_topcv_jobs trên db_staging thành file staging_<date>.sql
            // =========================
            new File(dumpFolder).mkdirs();
            dumpFile = dumpFolder + "/staging_" + dateParam + ".sql";

            String dumpCmd = String.format(
                    "mysqldump -h%s -P%d -u%s -p%s %s staging_topcv_jobs " +
                    "--where=\"DATE(extracted_date)='%s'\" --no-create-info --insert-ignore " +
                    "| sed 's/`staging_topcv_jobs`/`job_temp`/g' > %s",
                    stagingHost, stagingPort, stagingUser, stagingPass,
                    stagingDB, dateParam, dumpFile
            );

            runCommand(dumpCmd);

        } catch (Exception e) {
            long endTime = System.currentTimeMillis();
            insertLog(controlConn, dateParam, "Failed", 0, startTime, endTime, e.getMessage());
            throw e;

        } finally {
            if (controlConn != null) controlConn.close();
        }

        // =========================
        // Bước 6: Chuẩn bị danh sách tham số để gọi script shell (load_to_wh_with_retry.sh).
        // =========================
        List<String> cmd = Arrays.asList(
                "bash", loadScriptPath,
                configFile,
                dateParam,
                warehouseDB,
                warehouseUser,
                warehousePass,
                warehouseHost,
                String.valueOf(warehousePort),
                sshUser,
                targetPath,
                dumpFolder,
                controlDB,
                controlUser,
                controlPass,
                controlHost,
                String.valueOf(controlPort),
                dumpFile,
                String.valueOf(startTime)
        );

        runCommand(cmd);
    }

    // Hàm insert log (timestamp đã tự theo +7)
    private static void insertLog(Connection conn, String date, String status, int rows,
                                  long startMillis, long endMillis, String message) {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO load_to_wh_log(execution_date,data_date,status,rows_processed,start_time,end_time,message) VALUES(CURDATE(),?,?,?,?,?,?)")) {

            ps.setString(1, date);
            ps.setString(2, status);
            ps.setInt(3, rows);
            ps.setTimestamp(4, new Timestamp(startMillis));
            ps.setTimestamp(5, new Timestamp(endMillis));
            ps.setString(6, message);

            ps.executeUpdate();
            System.out.printf("%s%n", message);



        } catch (Exception e) {
            System.err.println("Failed to insert log: " + e.getMessage());
        }
    }

    private static void runCommand(List<String> command) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);
        Process p = pb.start();

        try (BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            br.lines().forEach(System.out::println);
        }

        int exitCode = p.waitFor();
        if (exitCode != 0) throw new RuntimeException("Command failed with exit code " + exitCode);
    }

    private static void runCommand(String cmd) throws Exception {
        runCommand(Arrays.asList("/bin/sh", "-c", cmd));
    }
}

