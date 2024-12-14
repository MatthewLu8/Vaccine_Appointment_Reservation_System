package scheduler;

import scheduler.db.ConnectionManager;
import scheduler.model.Caregiver;
import scheduler.model.Patient;
import scheduler.model.Vaccine;
import scheduler.util.Util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.sql.Date;
import java.util.*;

public class Scheduler {

    // objects to keep track of the currently logged-in user
    // Note: it is always true that at most one of currentCaregiver and currentPatient is not null
    //       since only one user can be logged-in at a time
    private static Caregiver currentCaregiver = null;
    private static Patient currentPatient = null;

    public static void main(String[] args) {
        // printing greetings text
        System.out.println();
        System.out.println("Welcome to the COVID-19 Vaccine Reservation Scheduling Application!");
        System.out.println("*** Please enter one of the following commands ***");
        System.out.println("> create_patient <username> <password>");  //TODO: implement create_patient (Part 1)
        System.out.println("> create_caregiver <username> <password>");
        System.out.println("> login_patient <username> <password>");  // TODO: implement login_patient (Part 1)
        System.out.println("> login_caregiver <username> <password>");
        System.out.println("> search_caregiver_schedule <date>");  // TODO: implement search_caregiver_schedule (Part 2)
        System.out.println("> reserve <date> <vaccine>");  // TODO: implement reserve (Part 2)
        System.out.println("> upload_availability <date>");
        System.out.println("> cancel <appointment_id>");  // TODO: implement cancel (extra credit)
        System.out.println("> add_doses <vaccine> <number>");
        System.out.println("> show_appointments");  // TODO: implement show_appointments (Part 2)
        System.out.println("> logout");  // TODO: implement logout (Part 2)
        System.out.println("> quit");
        System.out.println();

        // read input from user
        BufferedReader r = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            System.out.print("> ");
            String response = "";
            try {
                response = r.readLine();
            } catch (IOException e) {
                System.out.println("Please try again!");
            }
            // split the user input by spaces
            String[] tokens = response.split(" ");
            // check if input exists
            if (tokens.length == 0) {
                System.out.println("Please try again!");
                continue;
            }
            // determine which operation to perform
            String operation = tokens[0];
            if (operation.equals("create_patient")) {
                createPatient(tokens);
            } else if (operation.equals("create_caregiver")) {
                createCaregiver(tokens);
            } else if (operation.equals("login_patient")) {
                loginPatient(tokens);
            } else if (operation.equals("login_caregiver")) {
                loginCaregiver(tokens);
            } else if (operation.equals("search_caregiver_schedule")) {
                searchCaregiverSchedule(tokens);
            } else if (operation.equals("reserve")) {
                reserve(tokens);
            } else if (operation.equals("upload_availability")) {
                uploadAvailability(tokens);
            } else if (operation.equals("cancel")) {
                cancel(tokens);
            } else if (operation.equals("add_doses")) {
                addDoses(tokens);
            } else if (operation.equals("show_appointments")) {
                showAppointments(tokens);
            } else if (operation.equals("logout")) {
                logout(tokens);
            } else if (operation.equals("quit")) {
                System.out.println("Bye!");
                return;
            } else {
                System.out.println("Invalid operation name!");
            }
        }
    }

    private static void createPatient(String[] tokens) {
        // TODO: Part 1
        // create_caregiver <username> <password>
        // check 1: the length for tokens need to be exactly 3 to include all information (with the operation name)
        if (tokens.length != 3) {
            System.out.println("Create patient failed");
            return;
        }
        String username = tokens[1];
        String password = tokens[2];
        // check 2: check if the username has been taken already
        if (usernameExistsPatient(username)) {
            System.out.println("Username taken, try again!");
            return;
        }
        byte[] salt = Util.generateSalt();
        byte[] hash = Util.generateHash(password, salt);
        // create the caregiver
        try {
            Patient patient = new Patient.PatientBuilder(username, salt, hash).build();
            // save to caregiver information to our database
            patient.saveToDB();
            System.out.println("Created user " + username);
        } catch (SQLException e) {
            System.out.println("Create patient failed");
            e.printStackTrace();
        }
    }

    private static void createCaregiver(String[] tokens) {
        // create_caregiver <username> <password>
        // check 1: the length for tokens need to be exactly 3 to include all information (with the operation name)
        if (tokens.length != 3) {
            System.out.println("Failed to create user.");
            return;
        }
        String username = tokens[1];
        String password = tokens[2];
        // check 2: check if the username has been taken already
        if (usernameExistsCaregiver(username)) {
            System.out.println("Username taken, try again!");
            return;
        }
        byte[] salt = Util.generateSalt();
        byte[] hash = Util.generateHash(password, salt);
        // create the caregiver
        try {
            Caregiver caregiver = new Caregiver.CaregiverBuilder(username, salt, hash).build(); 
            // save to caregiver information to our database
            caregiver.saveToDB();
            System.out.println("Created user " + username);
        } catch (SQLException e) {
            System.out.println("Failed to create user.");
            e.printStackTrace();
        }
    }

    private static boolean usernameExistsCaregiver(String username) {
        ConnectionManager cm = new ConnectionManager();
        Connection con = cm.createConnection();

        String selectUsername = "SELECT * FROM Caregivers WHERE Username = ?";
        try {
            PreparedStatement statement = con.prepareStatement(selectUsername);
            statement.setString(1, username);
            ResultSet resultSet = statement.executeQuery();
            // returns false if the cursor is not before the first record or if there are no rows in the ResultSet.
            return resultSet.isBeforeFirst();
        } catch (SQLException e) {
            System.out.println("Error occurred when checking username");
            e.printStackTrace();
        } finally {
            cm.closeConnection();
        }
        return true;
    }

    private static boolean usernameExistsPatient(String username) {
        ConnectionManager cm = new ConnectionManager();
        Connection con = cm.createConnection();

        String selectUsername = "SELECT * FROM Patients WHERE Username = ?";
        try {
            PreparedStatement statement = con.prepareStatement(selectUsername);
            statement.setString(1, username);
            ResultSet resultSet = statement.executeQuery();
            // returns false if the cursor is not before the first record or if there are no rows in the ResultSet.
            return resultSet.isBeforeFirst();
        } catch (SQLException e) {
            System.out.println("Error occurred when checking username");
            e.printStackTrace();
        } finally {
            cm.closeConnection();
        }
        return true;
    }

    private static void loginPatient(String[] tokens) {
        // TODO: Part 1
        // login_patient <username> <password>
        // check 1: if someone's already logged-in, they need to log out first
        if (currentCaregiver != null || currentPatient != null) {
            System.out.println("User already logged in, try again.");
            return;
        }
        // check 2: the length for tokens need to be exactly 3 to include all information (with the operation name)
        if (tokens.length != 3) {
            System.out.println("Login patient failed.");
            return;
        }
        String username = tokens[1];
        String password = tokens[2];

        Patient patient = null;
        try {
            patient = new Patient.PatientGetter(username, password).get();
        } catch (SQLException e) {
            System.out.println("Login patient failed.");
            e.printStackTrace();
        }
        // check if the login was successful
        if (patient == null) {
            System.out.println("Login patient failed.");
        } else {
            System.out.println("Logged in as: " + username);
            currentPatient = patient;
        }
    }

    private static void loginCaregiver(String[] tokens) {
        // login_caregiver <username> <password>
        // check 1: if someone's already logged-in, they need to log out first
        if (currentCaregiver != null || currentPatient != null) {
            System.out.println("User already logged in.");
            return;
        }
        // check 2: the length for tokens need to be exactly 3 to include all information (with the operation name)
        if (tokens.length != 3) {
            System.out.println("Login failed.");
            return;
        }
        String username = tokens[1];
        String password = tokens[2];

        Caregiver caregiver = null;
        try {
            caregiver = new Caregiver.CaregiverGetter(username, password).get();
        } catch (SQLException e) {
            System.out.println("Login failed.");
            e.printStackTrace();
        }
        // check if the login was successful
        if (caregiver == null) {
            System.out.println("Login failed.");
        } else {
            System.out.println("Logged in as: " + username);
            currentCaregiver = caregiver;
        }
    }

    private static void searchCaregiverSchedule(String[] tokens) {
        // TODO: Part 2
        // search_caregiver_schedule <date>
        if (currentCaregiver == null && currentPatient == null){
            System.out.println("Please login first");
            return;
        }
        if(tokens.length != 2){
            System.out.println("Please try again");
            return;
        }

        try{

            ConnectionManager cm = new ConnectionManager();
            Connection con = cm.createConnection();

            String dateWhole = tokens[1];
            Date sqlDate = Date.valueOf(dateWhole);

            String getAvailableDates = "SELECT Time, Username FROM Availabilities WHERE Time = ? ORDER BY Username";
            String getVaccines = "SELECT Name, Doses FROM Vaccines";

            List<String> usernameResult = new LinkedList<>();
            List<String[]> vaccinesResult = new LinkedList<>();

            PreparedStatement statement = con.prepareStatement(getAvailableDates);
            statement.setDate(1, sqlDate);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                String username = resultSet.getString("Username");
                usernameResult.add(username);
            }

            PreparedStatement statement2 = con.prepareStatement(getVaccines);
            ResultSet resultSet2 = statement2.executeQuery();
            while (resultSet2.next()) {
                String availableDoses = resultSet2.getInt("Doses") + "";
                String vaccineName = resultSet2.getString("Name");
                String[] tempStringArray = new String[2];
                tempStringArray[0] = vaccineName;
                tempStringArray[1] = availableDoses;
                vaccinesResult.add(tempStringArray);
            }

            for (String username : usernameResult) {
                System.out.println(username);
            }

            for (String[] vaccineInformation : vaccinesResult) {
                System.out.println(vaccineInformation[0] + " " + vaccineInformation[1]);
            }

        }catch (Exception e){
            System.out.println("Please try again");
            return;
        }

    }

    private static void reserve(String[] tokens) {
        // TODO: Part 2
        // reserve <date> <vaccine>
        if (currentCaregiver == null && currentPatient == null){
            System.out.println("Please login first");
            return;
        }
        if(currentPatient == null){
            System.out.println("Please login as a patient");
            return;
        }
        if(tokens.length != 3){
            System.out.println("Please try again");
            return;
        }

        ConnectionManager cm = new ConnectionManager();
        Connection con = cm.createConnection();

        Date sqlDate = Date.valueOf(tokens[1]);

        String findAvailableDates = "SELECT TOP 1 Time, Username FROM Availabilities WHERE Time = ? ORDER BY Username";

        try{
            PreparedStatement statement = con.prepareStatement(findAvailableDates);
            statement.setDate(1, sqlDate);
            ResultSet resultSet = statement.executeQuery();

            if (!resultSet.next()) {
                System.out.println("No caregiver is available");
                return;
            }

            String assignedCaregiver = resultSet.getString("Username");
            String vaccineName = tokens[2];

            Vaccine vaccine = (new Vaccine.VaccineGetter(vaccineName)).get();

            if(vaccine.getAvailableDoses() == 0){
                System.out.println("Not enough available doses");
                return;
            }

            // 疫苗的有效数量-1
            vaccine.decreaseAvailableDoses(1);
            // 增加预约数
            String tempSql = "SELECT MAX(a_id) FROM Appointments";
            PreparedStatement statement2 = con.prepareStatement(tempSql);
            ResultSet resultSet2 = statement2.executeQuery();
            String addAppointment = "INSERT INTO Appointments VALUES (?, ?, ?, ?, ?)";
            PreparedStatement statement3 = con.prepareStatement(addAppointment);
            if (!resultSet2.next()) {
                statement3.setInt(1, 1);
                statement3.setDate(2, sqlDate);
                statement3.setString(3, currentPatient.getUsername());
                statement3.setString(4, assignedCaregiver);
                statement3.setString(5, vaccine.getVaccineName());
            }else{
                int highestRow = resultSet2.getInt(1);
                statement3.setInt(1, highestRow + 1);
                statement3.setDate(2, sqlDate);
                statement3.setString(3, currentPatient.getUsername());
                statement3.setString(4, assignedCaregiver);
                statement3.setString(5, vaccine.getVaccineName());
            }
            statement3.executeUpdate();
            // 更新caregiver's availability
            String dropAvailability = "DELETE FROM Availabilities WHERE Time = ? AND Username = ?";
            PreparedStatement statement4 = con.prepareStatement(dropAvailability);
            statement4.setDate(1, sqlDate);
            statement4.setString(2, assignedCaregiver);
            statement4.executeUpdate();

            // 记得添加事务的特征
            con.commit();

            String getAppointment = "SELECT a_id, date, c_username, vaccine_name FROM Appointments WHERE p_username = ? AND c_username = ? AND date = ?";
            PreparedStatement statement5 = con.prepareStatement(getAppointment);
            statement5.setString(1, currentPatient.getUsername());
            statement5.setString(2, assignedCaregiver);
            statement5.setDate(3, sqlDate);
            ResultSet resultSet5 = statement5.executeQuery();

            while (resultSet5.next()) {
                int appointmentID = resultSet5.getInt("a_id");
                String caregiverName = resultSet5.getString("c_username");
                System.out.println("Appointment ID " + appointmentID + ", Caregiver username " + caregiverName);
            }

        }catch (Exception e) {
            System.out.println("Please try again");
            return;
        }finally {
            cm.closeConnection();
        }

    }

    private static void uploadAvailability(String[] tokens) {
        // upload_availability <date>
        // check 1: check if the current logged-in user is a caregiver
        if (currentCaregiver == null) {
            System.out.println("Please login as a caregiver first!");
            return;
        }
        // check 2: the length for tokens need to be exactly 2 to include all information (with the operation name)
        if (tokens.length != 2) {
            System.out.println("Please try again!");
            return;
        }
        String date = tokens[1];
        try {
            Date d = Date.valueOf(date);
            currentCaregiver.uploadAvailability(d);
            System.out.println("Availability uploaded!");
        } catch (IllegalArgumentException e) {
            System.out.println("Please enter a valid date!");
        } catch (SQLException e) {
            System.out.println("Error occurred when uploading availability");
            e.printStackTrace();
        }
    }

    private static void cancel(String[] tokens) {
        // TODO: Extra credit
        // cancel <appointment_id>
        if (currentPatient == null && currentCaregiver == null) {
            System.out.println("Please login first!");
            return;
        }

        if (tokens.length != 2) {
            System.out.println("Failed to cancel appointment; wrong arguments given");
            return;
        }

        ConnectionManager cm = new ConnectionManager();
        Connection conn = cm.createConnection();
        if (conn == null) {
            System.out.println("Failed to connect to the database.");
            return;
        }

        String cancelId = tokens[1];
        boolean validAppointment = false;

        try {
            // 检查预约是否属于当前用户
            String getAppointmentSQL = "SELECT a_id, date, p_username, c_username, vaccine_name FROM Appointments WHERE a_id = ?";
            PreparedStatement getAppointmentStmt = conn.prepareStatement(getAppointmentSQL);
            getAppointmentStmt.setInt(1, Integer.parseInt(cancelId));

            ResultSet rs = getAppointmentStmt.executeQuery();
            Map<String, Object> appointment = null;
//            java.sql.Date temp_date = new Date(0);

            if (rs.next()) {
                appointment = new HashMap<>();
                appointment.put("a_id", rs.getInt("a_id"));
                appointment.put("date", rs.getDate("date"));
//                temp_date = rs.getDate("date");
                appointment.put("p_username", rs.getString("p_username"));
                appointment.put("c_username", rs.getString("c_username"));
                appointment.put("vaccine_name", rs.getString("vaccine_name"));
            }

            if (appointment == null) {
                System.out.println("Could not find appointment with id: " + cancelId);
                return;
            }

            // 验证预约是否属于当前用户
            if (currentPatient != null) {
                if (appointment.get("p_username").equals(currentPatient.getUsername())) {
                    validAppointment = true;
                } else {
                    System.out.println("Could not find appointment with id: " + cancelId);
                    return;
                }
            } else if (currentCaregiver != null) {
                if (appointment.get("c_username").equals(currentCaregiver.getUsername())) {
                    validAppointment = true;
                } else {
                    System.out.println("Could not find appointment with id: " + cancelId);
                    return;
                }
            }

            // 如果预约有效，则删除并恢复疫苗供应
            if (validAppointment) {
                // 删除预约
                String deleteAppointmentSQL = "DELETE FROM Appointments WHERE a_id = ?";
                PreparedStatement deleteAppointmentStmt = conn.prepareStatement(deleteAppointmentSQL);
                deleteAppointmentStmt.setInt(1, Integer.parseInt(cancelId));

                // 增加疫苗库存
                String vaccineName = (String) appointment.get("vaccine_name");
                Vaccine vaccine = (new Vaccine.VaccineGetter(vaccineName)).get();
                vaccine.increaseAvailableDoses(1); // 恢复疫苗供应

                deleteAppointmentStmt.executeUpdate();

//                Date appointmentDate = temp_date;
//                java.sql.Date appointmentDate = new java.sql.Date(((java.util.Date) appointment.get("date")).getTime());
                Date appointmentDate = (Date) appointment.get("date");
                String caregiver = (String) appointment.get("c_username");

                String insertAvailabilitySQL = "INSERT INTO Availabilities (Time, Username) VALUES (?, ?)";
                PreparedStatement insertAvailabilityStmt = conn.prepareStatement(insertAvailabilitySQL);
                insertAvailabilityStmt.setDate(1, appointmentDate);
                insertAvailabilityStmt.setString(2, caregiver);
                insertAvailabilityStmt.executeUpdate();

                conn.commit();
                System.out.println("Appointment successfully cancelled.");

            } else {
                System.out.println("Could not find appointment with id: " + cancelId);
            }
        } catch (SQLException e) {
            System.out.println("Failed to retrieve appointment information");
            e.printStackTrace();
        } finally {
            cm.closeConnection();
        }
    }

    private static void addDoses(String[] tokens) {
        // add_doses <vaccine> <number>
        // check 1: check if the current logged-in user is a caregiver
        if (currentCaregiver == null) {
            System.out.println("Please login as a caregiver first!");
            return;
        }
        // check 2: the length for tokens need to be exactly 3 to include all information (with the operation name)
        if (tokens.length != 3) {
            System.out.println("Please try again!");
            return;
        }
        String vaccineName = tokens[1];
        int doses = Integer.parseInt(tokens[2]);
        Vaccine vaccine = null;
        try {
            vaccine = new Vaccine.VaccineGetter(vaccineName).get();
        } catch (SQLException e) {
            System.out.println("Error occurred when adding doses");
            e.printStackTrace();
        }
        // check 3: if getter returns null, it means that we need to create the vaccine and insert it into the Vaccines
        //          table
        if (vaccine == null) {
            try {
                vaccine = new Vaccine.VaccineBuilder(vaccineName, doses).build();
                vaccine.saveToDB();
            } catch (SQLException e) {
                System.out.println("Error occurred when adding doses");
                e.printStackTrace();
            }
        } else {
            // if the vaccine is not null, meaning that the vaccine already exists in our table
            try {
                vaccine.increaseAvailableDoses(doses);
            } catch (SQLException e) {
                System.out.println("Error occurred when adding doses");
                e.printStackTrace();
            }
        }
        System.out.println("Doses updated!");
    }

    private static List<Map<String, Object>> fetchAppointments(ResultSet rs) throws SQLException {
        List<Map<String, Object>> appointments = new ArrayList<>();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (rs.next()) {
            Map<String, Object> row = new HashMap<>();
            for (int i = 1; i <= columnCount; i++) {
                row.put(metaData.getColumnLabel(i), rs.getObject(i));
            }
            appointments.add(row);
        }
        return appointments;
    }

    private static void showAppointments(String[] tokens) {
        // TODO: Part 2
        if (currentPatient == null && currentCaregiver == null) {
            System.out.println("Please login first");
            return;
        }

        if (tokens.length != 1) {
            System.out.println("Please try again");
            return;
        }

        ConnectionManager cm = new ConnectionManager();
        Connection conn = cm.createConnection();

        try {
            if (currentPatient != null) {
                // 获取患者的预约
                String getPatientAppointments = "SELECT a_id, vaccine_name, date, c_username FROM Appointments WHERE " +
                        "p_username = ? ORDER BY a_id";

                try (PreparedStatement stmt = conn.prepareStatement(getPatientAppointments)) {
                    stmt.setString(1, currentPatient.getUsername());
                    ResultSet rs = stmt.executeQuery();

                    List<Map<String, Object>> appointments = fetchAppointments(rs);

                    if (appointments.isEmpty()) {
                        System.out.println("There are no appointments scheduled");
                        return;
                    }

                    // 打印每个预约
                    for (Map<String, Object> appointment : appointments) {
                        System.out.printf("%-15s %-15s %-15s %-15s%n",
                                appointment.get("a_id"),
                                appointment.get("vaccine_name"),
                                appointment.get("date").toString(),
                                appointment.get("c_username"));
                    }
                }
            } else if (currentCaregiver != null) {
                // 获取护理人员的预约
                String getCaregiverAppointments = "SELECT a_id, vaccine_name, date, p_username FROM Appointments WHERE " +
                        "c_username = ? ORDER BY a_id";

                try (PreparedStatement stmt = conn.prepareStatement(getCaregiverAppointments)) {
                    stmt.setString(1, currentCaregiver.getUsername());
                    ResultSet rs = stmt.executeQuery();

                    List<Map<String, Object>> appointments = fetchAppointments(rs);

                    if (appointments.isEmpty()) {
                        System.out.println("There are no appointments scheduled");
                        return;
                    }

                    // 打印每个预约
                    for (Map<String, Object> appointment : appointments) {
                        System.out.printf("%-15s %-15s %-15s %-15s%n",
                                appointment.get("a_id"),
                                appointment.get("vaccine_name"),
                                appointment.get("date").toString(),
                                appointment.get("p_username"));
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("Please try again");
        } finally {
            cm.closeConnection();
        }
    }

    private static void logout(String[] tokens) {
        // TODO: Part 2
        try{
            if(currentPatient != null || currentCaregiver != null){
                currentPatient = null;
                currentCaregiver = null;
                System.out.println("Successfully logged out");
                return;
            }else{
                System.out.println("Please login first");
            }
        } catch (Exception e) {
            System.out.println("Please try again");
            return;
        }
    }
}
