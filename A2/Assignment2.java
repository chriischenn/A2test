import java.io.Console;
import java.sql.*;
import java.util.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Assignment2 {

  // A connection to the database
  Connection connection;

  // Can use if you wish: seat letters
  List<String> seatLetters = Arrays.asList("A", "B", "C", "D", "E", "F");

  Assignment2() throws SQLException {
    try {
      Class.forName("org.postgresql.Driver");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  /**
   * Connects and sets the search path.
   *
   * Establishes a connection to be used for this session, assigning it to the
   * instance variable 'connection'. In addition, sets the search path to
   * 'air_travel, public'.
   *
   * @param url      the url for the database
   * @param username the username to connect to the database
   * @param password the password to connect to the database
   * @return true if connecting is successful, false otherwise
   */
  public boolean connectDB(String URL, String username, String password) {
    // Implement this method!
    try {
      connection = DriverManager.getConnection(URL, username, password);
      if (connection != null) {
        return true;
      }
      return false;
    } catch (SQLException ex) {
      // database access error has occured or URL is null so we return false
      return false;
    }
  }

  /**
   * Closes the database connection.
   *
   * @return true if the closing was successful, false otherwise
   */
  public boolean disconnectDB() {
    // Implement this method!
    try {
      connection.close();
      return true;
    } catch (SQLException ex) {
      // connection was not closed properly, so return false
      return false;
    }
  }

  private ArrayList<PlaneSeat> getAllSeats(int econ_cap, int business_cap, int first_cap) {
    ArrayList<PlaneSeat> answer = new ArrayList<PlaneSeat>();
    int currentRow = 1;
    int currentSeat = 0;

    for (int i = 0; i < first_cap; i++) {
      PlaneSeat seat = new PlaneSeat();
      seat.className = "first";
      seat.letter = seatLetters.get(currentSeat);
      seat.row = currentRow;

      answer.add(seat);

      currentSeat++;
      if (currentSeat >= 6) {
        currentSeat = 0;
        currentRow++;
      }
    }

    if (currentSeat < 6) {
      currentRow++;
    }
    currentSeat = 0;

    // Repeat for bussiness
    for (int i = 0; i < business_cap; i++) {
      PlaneSeat seat = new PlaneSeat();
      seat.className = "business";
      seat.letter = seatLetters.get(currentSeat);
      seat.row = currentRow;

      answer.add(seat);

      currentSeat++;
      if (currentSeat >= 6) {
        currentSeat = 0;
        currentRow++;
      }
    }

    if (currentSeat < 6) {
      currentRow++;
    }
    currentSeat = 0;

    for (int i = 0; i < econ_cap; i++) {
      PlaneSeat seat = new PlaneSeat();
      seat.className = "economy";
      seat.letter = seatLetters.get(currentSeat);
      seat.row = currentRow;

      answer.add(seat);

      currentSeat++;
      if (currentSeat >= 6) {
        currentSeat = 0;
        currentRow++;
      }
    }
    return answer;
  }

  /* ======================= Airline-related methods ======================= */

  /**
   * Attempts to book a flight for a passenger in a particular seat class. Does so
   * by inserting a row into the Booking table.
   *
   * Read handout for information on how seats are booked. Returns false if seat
   * can't be booked, or if passenger or flight cannot be found.
   *
   *
   * @param passID    id of the passenger
   * @param flightID  id of the flight
   * @param seatClass the class of the seat (economy, business, or first)
   * @return true if the booking was successful, false otherwise.
   */
  public boolean bookSeat(int passID, int flightID, String seatClass) {
    // Implement this method!
    // System.out.println(String.format("flightID: %d, class: %s", flightID,
    // seatClass ));
    try {
        ResultSet rs;
        String flightInfo = "select row, letter " + "from air_travel.booking " + "where flight_id = ? "
            + "and seat_class = ?::air_travel.seat_class ";
        // String.format("and seat_class = %s ", seatClass);
        PreparedStatement flightInfoPS = connection.prepareStatement(flightInfo);
        flightInfoPS.setInt(1, flightID);
        flightInfoPS.setString(2, seatClass);
        rs = flightInfoPS.executeQuery();
        ArrayList<String> allBookedSeats = new ArrayList<String>();
        int overbookCount = 0;
        while (rs.next()) {
        int rowNum = rs.getInt("row");
        if (rs.wasNull()) {
            overbookCount++;
            continue;
        }
        String seatLetter = rs.getString("letter");
        String seatNum = String.format("%d%s", rowNum, seatLetter);
        allBookedSeats.add(seatNum);
        }

        String seatingInfo = "select capacity_economy, capacity_business, capacity_first "
            + "from air_travel.plane, air_travel.flight " + "where plane.tail_number=flight.plane and " + "flight.id= ? ";
        PreparedStatement seatingInfoPS = connection.prepareStatement(seatingInfo);
        seatingInfoPS.setInt(1, flightID);
        rs = seatingInfoPS.executeQuery();
        int econ_cap, business_cap, first_cap;
        if (rs.next()) {
        econ_cap = rs.getInt("capacity_economy");
        business_cap = rs.getInt("capacity_business");
        first_cap = rs.getInt("capacity_first");
        } else {
        return false;
        }

        // find price of booking
        String getPrice = "select * " + "from air_travel.price " + "where flight_id= ? ";
        PreparedStatement getPricePS = connection.prepareStatement(getPrice);
        getPricePS.setInt(1, flightID);
        rs = getPricePS.executeQuery();
        int flightPrice;
        if (rs.next()) {
        // System.out.println(String.format("flight id from price: %d",
        // rs.getInt("flight_id")));
        flightPrice = rs.getInt(seatClass);
        } else {
        return false;
        }

        //checking if departed
        String getDeparted = "SELECT timestamp " +
        "FROM air_travel.departed " +
        "WHERE flight_id = ?";
        PreparedStatement getDepartedPS = connection.prepareStatement(getDeparted);
        getDepartedPS.setInt(1, flightID);
        rs = getDepartedPS.executeQuery();
        Timestamp actualDeparted;
        if (rs.next()) {// if flight is in departed, we keep checking if it has left yet
          actualDeparted = rs.getTimestamp("timestamp");
          String getSched = "SELECT s_dep " +
          "FROM air_travel.flight " +
          "WHERE id = ?";
          PreparedStatement getSchedPS = connection.prepareStatement(getSched);
          getSchedPS.setInt(1, flightID);
          ResultSet schedRS = getSchedPS.executeQuery();
          Timestamp schedDeparted;
          if (schedRS.next()) {
            schedDeparted = schedRS.getTimestamp("s_dep");

            if (schedDeparted.before(actualDeparted)) {
              return false;
            }

          }

        }


        String newSeatLetter = null;
        int newRowNum = -1;

        if (seatClass == "economy") {
        // look through the a resulting relation of all ecomony booking to see if
        // less then 10 overbooked then we can book one more
        if (overbookCount >= 10) {
            return false;
        }

        // when there are overbooks, but the new passenger can still be booked
        // as an overbook
        if (overbookCount > 0) {
            newSeatLetter = null;
            newRowNum = -1;
        } else {
            for (PlaneSeat seat : this.getAllSeats(econ_cap, business_cap, first_cap)) {
            if (seat.className != seatClass) {
                continue;
            }

            if (allBookedSeats.contains(String.format("%d%s", seat.row, seat.letter))) {
                continue;
            }
            newSeatLetter = seat.letter;
            newRowNum = seat.row;
            break;
            }
            // not overbooked
            // we can book the new passenger, and assign a seat to them
        }
        String addPassenger = "insert into air_travel.booking values "
            + "((SELECT MAX(id)+1 from air_travel.booking), ?, ?, date_trunc('minute', NOW()), ?, ?::air_travel.seat_class, ?, ?)";
        PreparedStatement addPassengerPS = connection.prepareStatement(addPassenger);
        addPassengerPS.setInt(1, passID);
        addPassengerPS.setInt(2, flightID);
        addPassengerPS.setInt(3, flightPrice);
        addPassengerPS.setString(4, seatClass);
        if (newRowNum != -1) {
            addPassengerPS.setInt(5, newRowNum);
        } else {
            addPassengerPS.setNull(5, java.sql.Types.INTEGER);
        }
        if (newSeatLetter != null) {
            addPassengerPS.setString(6, newSeatLetter);
        } else {
            addPassengerPS.setNull(6, java.sql.Types.VARCHAR);
        }

        addPassengerPS.executeUpdate();
        return true;
        } else if (seatClass == "business" || seatClass == "first") {
        for (PlaneSeat seat : this.getAllSeats(econ_cap, business_cap, first_cap)) {
            if (seat.className != seatClass) {
            continue;
            }

            if (allBookedSeats.contains(String.format("%d%s", seat.row, seat.letter))) {
            continue;
            }
            newSeatLetter = seat.letter;
            newRowNum = seat.row;
            break;
        }
        if (newSeatLetter == null) {
            return false;
        }
        String addPassenger = "insert into air_travel.booking values "
            + "((SELECT MAX(id)+1 from air_travel.booking), ?, ?, date_trunc('minute', NOW()), ?, ?::air_travel.seat_class, ?, ?)";
        PreparedStatement addPassengerPS = connection.prepareStatement(addPassenger);
        addPassengerPS.setInt(1, passID);
        addPassengerPS.setInt(2, flightID);
        addPassengerPS.setInt(3, flightPrice);
        addPassengerPS.setString(4, seatClass);
        addPassengerPS.setInt(5, newRowNum);
        addPassengerPS.setString(6, newSeatLetter);
        addPassengerPS.executeUpdate();
        return true;

        }
    }
    catch (SQLException e) {
        return false;
    }

    return false;
  }

  /**
   * Attempts to upgrade overbooked economy passengers to business class or first
   * class (in that order until each seat class is filled). Does so by altering
   * the database records for the bookings such that the seat and seat_class are
   * updated if an upgrade can be processed.
   *
   * Upgrades should happen in order of earliest booking timestamp first.
   *
   * If economy passengers left over without a seat (i.e. more than 10 overbooked
   * passengers or not enough higher class seats), remove their bookings from the
   * database.
   *
   * @param flightID The flight to upgrade passengers in.
   * @return the number of passengers upgraded, or -1 if an error occured.
   */
  public int upgrade(int flightID) {
    try {
        ResultSet rs;
        String flightInfo = "select id, seat_class, row, letter from air_travel.booking "
        + "where flight_id = ? ORDER BY datetime ASC";
        // String.format("and seat_class = %s ", seatClass);
        PreparedStatement flightInfoPS = connection.prepareStatement(flightInfo);
        flightInfoPS.setInt(1, flightID);
        rs = flightInfoPS.executeQuery();
        ArrayList<String> allBookedSeats = new ArrayList<String>();
        ArrayList<Integer> toUpgrade = new ArrayList<Integer>();
        while (rs.next()) {
        int rowNum = rs.getInt("row");
        if (rs.wasNull()) {
            toUpgrade.add(rs.getInt("id"));
            continue;
        }
        String seatLetter = rs.getString("letter");
        String seatNum = String.format("%d%s", rowNum, seatLetter);
        allBookedSeats.add(seatNum);
        }

        String seatingInfo = "select capacity_economy, capacity_business, capacity_first "
            + "from air_travel.plane, air_travel.flight " + "where plane.tail_number=flight.plane and " + "flight.id= ? ";
        PreparedStatement seatingInfoPS = connection.prepareStatement(seatingInfo);
        seatingInfoPS.setInt(1, flightID);
        rs = seatingInfoPS.executeQuery();
        int econ_cap, business_cap, first_cap;
        if (rs.next()) {
        econ_cap = rs.getInt("capacity_economy");
        business_cap = rs.getInt("capacity_business");
        first_cap = rs.getInt("capacity_first");
        } else {
        return 0;
        }

        int upgraded = 0;

        for (PlaneSeat seat : this.getAllSeats(econ_cap, business_cap, first_cap)) {
        if (seat.className != "business") {
            continue;
        }

        if (allBookedSeats.contains(String.format("%d%s", seat.row, seat.letter))) {
            continue;
        }

        if(toUpgrade.size() == 0) {
            break;
        }

        int upgradedId = toUpgrade.remove(0);
        String updatePassenger = "UPDATE air_travel.booking SET row=?, letter=?, seat_class=?::air_travel.seat_class WHERE id=?";
        PreparedStatement updatePassengerPS = connection.prepareStatement(updatePassenger);
        updatePassengerPS.setInt(1, seat.row);
        updatePassengerPS.setString(2, seat.letter);
        updatePassengerPS.setString(3, seat.className);
        updatePassengerPS.setInt(4, upgradedId);
        updatePassengerPS.executeUpdate();
        upgraded++;
        }


        for (PlaneSeat seat : this.getAllSeats(econ_cap, business_cap, first_cap)) {
        if (seat.className != "first") {
            continue;
        }

        if (allBookedSeats.contains(String.format("%d%s", seat.row, seat.letter))) {
            continue;
        }

        if(toUpgrade.size() == 0) {
            break;
        }

        int upgradedId = toUpgrade.remove(0);
        String updatePassenger = "UPDATE air_travel.booking SET row=?, letter=?, seat_class=?::air_travel.seat_class WHERE id=?";
        PreparedStatement updatePassengerPS = connection.prepareStatement(updatePassenger);
        updatePassengerPS.setInt(1, seat.row);
        updatePassengerPS.setString(2, seat.letter);
        updatePassengerPS.setString(3, seat.className);
        updatePassengerPS.setInt(4, upgradedId);
        updatePassengerPS.executeUpdate();
        upgraded++;
        }

        String deletePassangers = "DELETE FROM air_travel.booking WHERE row IS NULL AND flight_id = ?";
        PreparedStatement deletePassangersPS = connection.prepareStatement(deletePassangers);
        deletePassangersPS.setInt(1, flightID);
        deletePassangersPS.executeUpdate();

        return upgraded;
    } catch (SQLException e) {
        return -1;
    }
  }

  /* ----------------------- Helper functions below ------------------------- */

  // A helpful function for adding a timestamp to new bookings.
  // Example of setting a timestamp in a PreparedStatement:
  // ps.setTimestamp(1, getCurrentTimeStamp());

  /**
   * Returns a SQL Timestamp object of the current time.
   *
   * @return Timestamp of current time.
   */
  private java.sql.Timestamp getCurrentTimeStamp() {
    java.util.Date now = new java.util.Date();
    return new java.sql.Timestamp(now.getTime());
  }

  // Add more helper functions below if desired.

  /* ----------------------- Main method below ------------------------- */

  public static void main(String[] args) throws SQLException {
    // You can put testing code in here. It will not affect our autotester.
    PlaneSeat.main(null);
  }

}

class PlaneSeat {
  String className;
  String letter;
  int row;

  public static void main(String[] s) throws SQLException {
    System.out.println("Running the code!");
    String url = "jdbc:postgresql://localhost:5432/csc343h-chenha53";
    Assignment2 a2 = new Assignment2();
    if (a2.connectDB(url, "chenha53", "")) {
      System.out.println("db connection successful");
    } else {
      System.out.println("db connection failed");
    }

    //int r = a2.upgrade(5);
    //if (r != -1) {
      //System.out.println(String.format("upgrade: %d", r));
    //} else {
      //System.out.println("error");
    //}

    boolean bookedF = a2.bookSeat(1, 6, "first");
    if (bookedF == true) {
      System.out.println("Booked Passenger 6 on flight 5");
    } else {
      System.out.println("Flight 5 has already left");
    }

    if (a2.disconnectDB()) {
      System.out.println("db disconnection successful");
    } else {
      System.out.println("db disconnection failed");
    }
  }
}
