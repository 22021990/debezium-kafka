import org.h2.tools.Server;

import java.sql.*;
import java.util.LinkedHashMap;

public class H2Database {

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
            Class.forName("org.h2.Driver");
            String DB_CONNECTION = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1";
            String DB_USER = "sa";
            String DB_PASSWORD = "";
            dbConnection = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
        } catch (SQLException | ClassNotFoundException e) {
            System.out.println(e.getMessage());
        }

        return dbConnection;
    }

    private void createTableStudent() {
        try (Connection connection = getDBConnection()) {
            PreparedStatement createPreparedStatement;
            String CreateQuery = "" +
                    "CREATE TABLE STUDENT(" +
                    "gde_id int PRIMARY KEY AUTO_INCREMENT, " +
                    "gde_timestamp timestamp, " +
                    "gde_event_type varchar(255), " +
                    "student_table_id int, " +
                    "student_table_first_name varchar(255), " +
                    "student_table_last_name varchar(255), " +
                    "student_table_email varchar(255))";
            connection.setAutoCommit(false);

            createPreparedStatement = connection.prepareStatement(CreateQuery);
            createPreparedStatement.executeUpdate();
            createPreparedStatement.close();
            connection.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void createTableAirport() {
        try (Connection connection = getDBConnection()) {
            PreparedStatement createPreparedStatement;
            String CreateQuery = "" +
                    "CREATE TABLE AIRPORT(" +
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
            connection.setAutoCommit(false);
            createPreparedStatement = connection.prepareStatement(CreateQuery);
            createPreparedStatement.executeUpdate();
            createPreparedStatement.close();
            connection.commit();
        } catch (Exception e) {
            e.printStackTrace();
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

    private void generateAndExecuteInsert(LinkedHashMap<String, String> sourceKeyValuePairs, LinkedHashMap<String, String> dataToInsert, String typeOfOperation) {
        if (conn == null) {
            conn = getDBConnection();
            try {
                conn.setAutoCommit(false);
            } catch (Exception exc) {
                exc.printStackTrace();
            }
        }

        StringBuilder insertQuery = new StringBuilder();
        insertQuery.append("INSERT INTO ")
                .append(sourceKeyValuePairs.get("table").toUpperCase())
                .append(" VALUES(")
                .append("DEFAULT,") // generated id
                .append("'")
                .append(new Timestamp(System.currentTimeMillis()))
                .append("',")
                .append("'")
                .append(typeOfOperation)
                .append("',");
        dataToInsert.forEach((key, value) -> insertQuery.append(value).append(","));
        insertQuery.replace(insertQuery.length() - 1, insertQuery.length(), ")");

        Statement insertStatement = null;
        try {
            insertStatement = conn.createStatement();
            insertStatement.execute(insertQuery.toString());
            conn.commit();
        } catch (Exception exc) {
            exc.printStackTrace();
        } finally {
            try {
                assert insertStatement != null;
                insertStatement.close();
            } catch (Exception exc) {
                exc.printStackTrace();
            }
        }
    }

    // vise genericki metod od extractSourceKeyValuePairs metoda. extractSourceKeyValuePairs sam prvo napisao i shvatio da jedan metod moze raditi izvlacenje za sva tri polja: before, after i source.
    private LinkedHashMap<String, String> extractKeyValuePairs(String eventValue, String fieldKey) {
        int addToStart = 0;
        String replacingQuotationMarkWith = "";

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
                break;
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
            // if value contains ' then add another one so it escapses insert into h2 database -> .replaceAll("'", "''");
            fieldKeyValuePairs.put(
                    keyValuePair.substring(0, indexOfColon).replaceAll("\"", replacingQuotationMarkWith),
                    keyValuePair.substring(indexOfColon + 1).replaceAll("\"", replacingQuotationMarkWith)
            );
        }

        return fieldKeyValuePairs;
    }
}
