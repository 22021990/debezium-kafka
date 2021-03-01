import org.h2.tools.Server;

import java.sql.*;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class H2Database {

    private final String DB_DRIVER = "org.h2.Driver";
    private final String DB_CONNECTION = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1";
    private final String DB_USER = "sa";
    private final String DB_PASSWORD = "";

    private Connection conn = null;

    /*
    before ce uvek ici prvi tako da mozemo i ovako da ga testiramo a after i source sa ',' ispred i ':{' iza obavezno.
    zasto? zasto smo mozemo imati polje na nazivom source ili after u beforu ili cak i u after/source-u.
    ovako smo sigurni da ce index za ovaj string biti index pocetka JSON objekta koji nam oznava stanje objekta iz baze.
     */
    private static final String EXTRACT_DEBEZIUM_BEFORE = "{\"before\":";
    private static final String EXTRACT_DEBEZIUM_AFTER  = ",\"after\":{";
    private static final String EXTRACT_DEBEZIUM_SOURCE = ",\"source\":{";

    private static final String GDE_EVENT_TYPE_CREATE = "CREATE";
    private static final String GDE_EVENT_TYPE_UPDATE = "UPDATE";
    private static final String GDE_EVENT_TYPE_DELETE = "DELETE";

    public H2Database() {
        try {
            openServerModeInBrowser();
            createTableStudent();
            createTableAirport();
        } catch (Exception exc) {
            exc.printStackTrace();
        }
    }

    private void openServerModeInBrowser() throws SQLException {
        Server server = Server.createTcpServer().start();
        System.out.println("Server started and connection is open.");
        System.out.println("URL: jdbc:h2:" + server.getURL() + "/mem:test");
    }

    private Connection getDBConnection() {
        Connection dbConnection = null;
        try {
            Class.forName(DB_DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println(e.getMessage());
        }
        try {
            dbConnection = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
            return dbConnection;
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return dbConnection;
    }

    private void createTableStudent() throws SQLException {
        Connection connection = getDBConnection();
        PreparedStatement createPreparedStatement = null;

        String CreateQuery = "CREATE TABLE STUDENT(" +
                "gde_id int PRIMARY KEY AUTO_INCREMENT, " +
                "gde_timestamp timestamp, " +
                "gde_event_type varchar(255), " +
                "student_table_id int, " +
                "student_table_first_name varchar(255), " +
                "student_table_last_name varchar(255), " +
                "student_table_email varchar(255))";

        try {
            connection.setAutoCommit(false);

            createPreparedStatement = connection.prepareStatement(CreateQuery);
            createPreparedStatement.executeUpdate();
            createPreparedStatement.close();

            connection.commit();
        } catch (Exception e) {
            System.out.println("Exception Message " + e.getLocalizedMessage());
        } finally {
            connection.close();
        }
    }

    private void createTableAirport() throws SQLException {
        Connection connection = getDBConnection();
        PreparedStatement createPreparedStatement = null;

        String CreateQuery = "CREATE TABLE AIRPORT(" +
                "gde_id int PRIMARY KEY AUTO_INCREMENT, " +
                "gde_timestamp timestamp, " +
                "gde_event_type varchar(255), " +
                "source_table_id int, " +
                "source_table_airport_id int, " +
                "source_table_name varchar(255), " +
                "source_table_city varchar(255), " +
                "source_table_country varchar(255), " +
                "source_table_iata varchar(255), " +
                "source_table_icao varchar(255), " +
                "source_table_latitude double, " +
                "source_table_longitude double, " +
                "source_table_altitude int, " +
                "source_table_timezone double, " +
                "source_table_dst varchar(255), " +
                "source_table_tz varchar(255), " +
                "source_table_type varchar(255), " +
                "source_table_source varchar(255), " +
                "source_table_reg_date varchar(255)" + //date
                ")";

        try {
            connection.setAutoCommit(false);

            createPreparedStatement = connection.prepareStatement(CreateQuery);
            createPreparedStatement.executeUpdate();
            createPreparedStatement.close();

            connection.commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            connection.close();
        }
    }

    public void processKafkaStringAndInsertIntoDatabase(String eventValueFromKafka) throws Exception {
        // kod delete eventa, posle 'pravog' eventa dolazi nam jos jedan sa vrednoscu null i njega treba ignorisati.
        if(eventValueFromKafka == null) {
            return;
        }

        // poslednja zagrada sa zarezom nam je od source objekta, posle toga idu 'op', 'ts' i 'transaction' polja.
        int indexOfLastBracket = eventValueFromKafka.lastIndexOf("},");

        // uzimamo samo deo JSON-a posle poslednje }, tj. deo sa 'op' poljem.
        String partOfValueStringWithOpField = eventValueFromKafka.substring(indexOfLastBracket);

        // izvlazimo iz stringa vrednost 'op' polja, to mozemo da radimo na ovaj nacin sa sigurnoscu jer ce uvek posle }, ici kljuc 'op' pa njegova vrednost u istom formatu i duzini.
        String typeOfEvent = partOfValueStringWithOpField.substring(8, 9);

        // u zavistnosti od vrednosti 'op' polja, nastavljamo da procesuiramo string na razlicite nacine jer je i vrednost JSON objekta koji je u stringu drugacija za sve tri opcije.
        switch (typeOfEvent) {
            case "c":
                processCreateEvent(eventValueFromKafka);
                break;
            case "u":
                processUpdateEvent(eventValueFromKafka);
                break;
            case "d":
                processDeleteEvent(eventValueFromKafka);
                break;

            default:
                throw new Exception("Vrednost 'op' polja nije c, u ili d!");
        }
    }

    /*
        Example of create event for Airport table.

        {
           "before":null,
           "after":{
              "id":28889,
              "airport_id":1,
              "name":"Goroka Airport",
              "city":"Goroka",
              "country":"Papua New Guinea",
              "iata":"GKA",
              "icao":"AYGA",
              "latitude":-6.081689834590001,
              "longitude":145.391998291,
              "altitude":5282,
              "timezone":10,
              "dst":"U",
              "tz":"Pacific/Port_Moresby",
              "type":"airport",
              "source":"OurAirports",
              "reg_date":"2021-02-27T18:09:06Z"
           },
           "source":{
              "version":"1.4.1.Final",
              "connector":"mysql",
              "name":"debezium-kafka-project",
              "ts_ms":1614452945000,
              "snapshot":"false",
              "db":"hb_student_tracker",
              "table":"airport",
              "server_id":1,
              "gtid":null,
              "file":"CDS-STEFANS-bin.000003",
              "pos":12455092,
              "row":0,
              "thread":139,
              "query":null
           },
           "op":"c",
           "ts_ms":1614452945951,
           "transaction":null
        }
     */
    private void processCreateEvent(String eventValue) {
        LinkedHashMap<String, String> sourceKeyValuePairs = extractKeyValuePairs(eventValue, EXTRACT_DEBEZIUM_SOURCE);
        LinkedHashMap<String, String> afterKeyValuePairs = extractKeyValuePairs(eventValue, EXTRACT_DEBEZIUM_AFTER);

        generateAndExecuteInsert(sourceKeyValuePairs, afterKeyValuePairs, GDE_EVENT_TYPE_CREATE);
    }

    private void processUpdateEvent(String eventValue) {
        LinkedHashMap<String, String> sourceKeyValuePairs = extractKeyValuePairs(eventValue, EXTRACT_DEBEZIUM_SOURCE);
        LinkedHashMap<String, String> afterKeyValuePairs = extractKeyValuePairs(eventValue, EXTRACT_DEBEZIUM_AFTER);

        generateAndExecuteInsert(sourceKeyValuePairs, afterKeyValuePairs, GDE_EVENT_TYPE_UPDATE);
    }

    private void processDeleteEvent(String eventValue) {
        LinkedHashMap<String, String> sourceKeyValuePairs = extractKeyValuePairs(eventValue, EXTRACT_DEBEZIUM_SOURCE);
        LinkedHashMap<String, String> beforeKeyValuePairs = extractKeyValuePairs(eventValue, EXTRACT_DEBEZIUM_BEFORE);

        generateAndExecuteInsert(sourceKeyValuePairs, beforeKeyValuePairs, GDE_EVENT_TYPE_DELETE);
    }

    //INSERT INTO AIRPORT values(12,'2021-02-26 15:30:42.441','CREATE',28841,6,'Wewak International Airport','Wewak','Papua New Guinea','WWK','AYWK',-3.58383011818,143.669006348,19,10.0,'U','Pacific/Port_Moresby','airport', 'OurAirports','2021-02-26')
    private void generateAndExecuteInsert(LinkedHashMap<String, String> sourceKeyValuePairs, LinkedHashMap<String, String> dataToInsert, String typeOfOperation) {
        if (conn == null) {
            conn = getDBConnection();
            try {
                conn.setAutoCommit(false);
            } catch (Exception exc) {
                exc.printStackTrace();
            }
        }

        Set<String> setKeys = dataToInsert.keySet();
        StringBuilder insertQuery = new StringBuilder();
        insertQuery.append("" +
                "INSERT INTO " + sourceKeyValuePairs.get("table").toUpperCase() + " VALUES(" +
                "DEFAULT," + // id
                "'" + new Timestamp(System.currentTimeMillis()) + "'," +
                "'" + typeOfOperation + "',"
        );
        dataToInsert.entrySet().forEach(v -> insertQuery.append(v.getValue() + ","));
        insertQuery.replace(insertQuery.length() - 1, insertQuery.length(), ")");

        //INSERT INTO AIRPORT(gde_timestamp, gde_event_type, source_table_id,source_table_airport_id,source_table_name,source_table_city,source_table_country,source_table_iata,source_table_icao,source_table_latitude,source_table_longitude,source_table_altitude,source_table_timezone,source_table_dst,source_table_tz,source_table_type,'source_table_source,source_table_reg_date) VALUES('2021-03-01 10:26:41.103','CREATE',93368,3,'Mount Hagen Kagamuga Airport','Mount Hagen','Papua New Guinea','HGU','AYMH',-5.826789855957031,144.29600524902344,5388,10,'U','Pacific/Port_Moresby','airport','OurAirports','2021-03-01T08:24:33Z')
        Statement insertStatement = null;
        try {
            insertStatement = conn.createStatement();
            insertStatement.execute(insertQuery.toString());
            conn.commit();
        } catch (Exception exc) {
            exc.printStackTrace();
        } finally {
            try {
                insertStatement.close();
            } catch (Exception exc) {
                exc.printStackTrace();
            }
        }
    }

    // vise genericki metod od extractSourceKeyValuePairs metoda. extractSourceKeyValuePairs sam prvo napisao i shvatio da jedan metod moze raditi izvlacenje za sva tri polja: before, after i source.
    private LinkedHashMap<String, String> extractKeyValuePairs(String eventValue, String fieldKey) {
        Integer addToStart = null;
        String replacingQuotationMarkWith = null;

        switch (fieldKey) {
            case EXTRACT_DEBEZIUM_SOURCE:
                addToStart = 11;
                replacingQuotationMarkWith = "";
                break;
            case EXTRACT_DEBEZIUM_AFTER:
                addToStart = 10;
                replacingQuotationMarkWith = "'";
                break;
            case EXTRACT_DEBEZIUM_BEFORE:
                addToStart = 11;
                replacingQuotationMarkWith = "'";
                break;

            default:
                System.out.println("ERROR");
        }

        // +11 to include ,"source":{ when performing substring, so we are left only with key/value pairs. It will always be +11
        int indexOfFieldStart = eventValue.indexOf(fieldKey) + addToStart;

        // izdvojimo samo deo vezan za vrednost polja koje izvlacimo.
        String eventValueShortened = eventValue.substring(indexOfFieldStart);
        int indexOfBracket = eventValueShortened.indexOf("},");

        // odstranimo sve osim onoga sto se nalazilo unutra before/after/source zagrada -> "source":{}
        String fieldKeyValues = eventValueShortened.substring(0, indexOfBracket);

        // podeli string po ',' i ostace nam key/value parovi vrednosti polja.
        String[] fieldKeyValuePairsArray = fieldKeyValues.split(",");

        LinkedHashMap<String, String> fieldKeyValuePairs = new LinkedHashMap<>();
        for (String keyValuePair : fieldKeyValuePairsArray) {
            int indexOfColon = keyValuePair.indexOf(":");
            // if value contains ' then add another one so it escapses insert into h2 database.
            fieldKeyValuePairs.put(
                    keyValuePair.substring(0, indexOfColon).replaceAll("\"", replacingQuotationMarkWith),
                    keyValuePair.substring(indexOfColon + 1).replaceAll("\"", replacingQuotationMarkWith)
            );
        }

//        fieldKeyValuePairs.entrySet().forEach(entry -> System.out.println(entry.getKey() + " " + entry.getValue()));
        return fieldKeyValuePairs;
    }

    /*
    testni metod napisan radi razumevanja i citljivosti podatakaa.
    u produkciji umesto mape koristicemo obican string, kada budemo prolazili kroz array sa vrednostima, red koji sadrzi "table" u sebi,
    iz njega cemo izvuci vrednost tog polja i uraditi break;, i vratiti tu vrednost kao ime tabele u koju trebamo da upisujemo na sink strani.
     */
    private LinkedHashMap<String, String> extractSourceKeyValuePairs(String eventValue) {
        // +11 to include ,"source":{ when performing substring, so we are left only with key/value pairs. It will always be +11
        int indexOfSource = eventValue.indexOf(",\"source\":{") + 11;
        int indexOfLastBracket = eventValue.lastIndexOf("},");

        // odstranimo sve osim onoga sto se nalazilo unutra source zagrada -> "source":{}
        String sourceKeyValues = eventValue.substring(indexOfSource, indexOfLastBracket);

        // "version":"1.4.1.Final","connector":"mysql","name":"steffff-test9","ts_ms":1614452945000,"snapshot":"false","db":"hb_student_tracker","table":"airport","server_id":1,"gtid":null,"file":"CDS-STEFANS-bin.000003","pos":12455092,"row":0,"thread":139,"query":null
        // podeli string po ',' i ostace nam key/value pairs source vrednosti.
        String[] sourceKeyValuePairsArray = sourceKeyValues.split(",");

        // spakujemo sve key/value parove u mapu. ovde ovo radim cisto zbog testiranja ali u principu za nas projekat nam je dovoljno da nadjemo string koji sadrzi "table" i izvucemo samo njegovu vrednost, odradimo break; i vratimo string umesto mape sto ce biti dosta brze i zauzimati manje memorije.
        LinkedHashMap<String, String> sourceKeyValuePairs = new LinkedHashMap<>();
        for (String keyValuePair : sourceKeyValuePairsArray) {
            int indexOfColon = keyValuePair.indexOf(":");
            sourceKeyValuePairs.put(
                    keyValuePair.substring(0, indexOfColon).replaceAll("\"", ""),
                    keyValuePair.substring(indexOfColon + 1).replaceAll("\"", "")
            );
        }

//        sourceKeyValuePairs.entrySet().forEach(entry -> System.out.println(entry.getKey() + " " + entry.getValue()));
        return sourceKeyValuePairs;
    }

}
