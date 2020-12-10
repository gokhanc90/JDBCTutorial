package ceng.estu;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Hello world!
 *
 */
public class JDBCExamples
{
    Connection con;
    public JDBCExamples() {
        con = Utils.getConnection();
    }

    public static void main(String[] args ) throws SQLException {
        JDBCExamples tutorial = new JDBCExamples();
        //Select example
        tutorial.viewTable();
        System.out.println();

        //Update Example
        HashMap<String, Integer> salesCoffeeWeek =
                new HashMap<String, Integer>();
        salesCoffeeWeek.put("Colombian", 175);
        salesCoffeeWeek.put("French_Roast", 150);
        salesCoffeeWeek.put("Espresso", 100);
        salesCoffeeWeek.put("Colombian_Decaf", 155);
        salesCoffeeWeek.put("French_Roast_Decaf", 90);
        tutorial.updateCoffeeSales(salesCoffeeWeek);
        tutorial.viewTable();
        System.out.println();

        System.out.println("Raising coffee prices by 25%");
        tutorial.modifyPrices(1.25f);
        tutorial.viewTable();

        System.out.println("\nInserting a new row:");
        //tutorial.insertRow("Kona", 150, 10.99f, 0, 0);
        tutorial.viewTable();

        System.out.println("\nDelete :Kona");
        tutorial.deleteRow("Kona");
        tutorial.viewTable();

    }
    public void insertRow(String coffeeName, int supplierID, float price,
                          int sales, int total)  {

        try (Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE))
        {
            ResultSet uprs = stmt.executeQuery("SELECT * FROM COFFEES");
            uprs.moveToInsertRow();
            uprs.updateString("COF_NAME", coffeeName);
            uprs.updateInt("SUP_ID", supplierID);
            uprs.updateFloat("PRICE", price);
            uprs.updateInt("SALES", sales);
            uprs.updateInt("TOTAL", total);

            uprs.insertRow();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void deleteRow(String coffeeName){
        String deleteString = "DELETE FROM COFFEES " +
                "WHERE COF_NAME = ?";
        try (PreparedStatement deleteCoffee = con.prepareStatement(deleteString))
        {
            deleteCoffee.setString(1,coffeeName);
            deleteCoffee.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /***
     * PreparedStatement accepts input parameter.
     * executeUpdate executes the SQL statement in PreparedStatement object,
     * which must be an SQL Data Manipulation Language (DML) statement, such as INSERT, UPDATE or
     * DELETE or an SQL statement that returns nothing,
     * @param salesForWeek
     * @throws SQLException
     */
    public void updateCoffeeSales(HashMap<String, Integer> salesForWeek) throws SQLException {
        String updateString =
                "update COFFEES set SALES = ? where COF_NAME = ?";
        String updateStatement =
                "update COFFEES set TOTAL = TOTAL + ? where COF_NAME = ?";

        try (PreparedStatement updateSales = con.prepareStatement(updateString);
             PreparedStatement updateTotal = con.prepareStatement(updateStatement))

        {
            con.setAutoCommit(false);
            for (Map.Entry<String, Integer> e : salesForWeek.entrySet()) {
                updateSales.setInt(1, e.getValue().intValue());
                updateSales.setString(2, e.getKey());
                updateSales.executeUpdate();

                updateTotal.setInt(1, e.getValue().intValue());
                updateTotal.setString(2, e.getKey());
                updateTotal.executeUpdate();
                con.commit();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            if (con != null) {
                try {
                    System.err.print("Transaction is being rolled back");
                    con.rollback();
                } catch (SQLException excep) {
                    e.printStackTrace();
                }
            }
        }
        con.setAutoCommit(true);
    }


    public void modifyPrices(float percentage) throws SQLException {
        try (Statement stmt =
                     con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
            ResultSet uprs = stmt.executeQuery("SELECT * FROM COFFEES");
            while (uprs.next()) {
                float f = uprs.getFloat("PRICE");
                uprs.updateFloat("PRICE", f * percentage);
                uprs.updateRow();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void modifyPricesByPercentage(
            String coffeeName,
            float priceModifier,
            float maximumPrice) throws SQLException {
        con.setAutoCommit(false);
        ResultSet rs = null;
        String priceQuery = "SELECT COF_NAME, PRICE FROM COFFEES " +
                "WHERE COF_NAME = ?";
        String updateQuery = "UPDATE COFFEES SET PRICE = ? " +
                "WHERE COF_NAME = ?";
        try (PreparedStatement getPrice = con.prepareStatement(priceQuery, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
             PreparedStatement updatePrice = con.prepareStatement(updateQuery))
        {
            Savepoint save1 = con.setSavepoint();
            getPrice.setString(1, coffeeName);
            if (!getPrice.execute()) {
                System.out.println("Could not find entry for coffee named " + coffeeName);
            } else {
                rs = getPrice.getResultSet();
                rs.first();
                float oldPrice = rs.getFloat("PRICE");
                float newPrice = oldPrice + (oldPrice * priceModifier);
                System.out.printf("Old price of %s is $%.2f%n", coffeeName, oldPrice);
                System.out.printf("New price of %s is $%.2f%n", coffeeName, newPrice);
                System.out.println("Performing update...");
                updatePrice.setFloat(1, newPrice);
                updatePrice.setString(2, coffeeName);
                updatePrice.executeUpdate();
                System.out.println("\nCOFFEES table after update:");
                viewTable(con);
                if (newPrice > maximumPrice) {
                    System.out.printf("The new price, $%.2f, is greater " +
                                    "than the maximum price, $%.2f. " +
                                    "Rolling back the transaction...%n",
                            newPrice, maximumPrice);
                    con.rollback(save1);
                    System.out.println("\nCOFFEES table after rollback:");
                    viewTable(con);
                }
                con.commit();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            con.setAutoCommit(true);
        }

    }

    public void viewTable() throws SQLException{
        viewTable(con);
    }

    /**
     * Statement enables you to send SQL query to your database and it does not take any parameter.
     * executeQuery executes the given SQL statement, which returns a single ResultSet object. Useful for SELECT statement.
     * @param con
     * @throws SQLException
     */
    public void viewTable(Connection con) throws SQLException {
        String query = "select COF_NAME, SUP_ID, PRICE, SALES, TOTAL from COFFEES";
        System.out.println("COF_NAME SUP_ID  PRICE  SALES  TOTAL");
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                String coffeeName = rs.getString("COF_NAME");
                int supplierID = rs.getInt("SUP_ID");
                float price = rs.getFloat("PRICE");
                int sales = rs.getInt("SALES");
                int total = rs.getInt("TOTAL");
                System.out.println(coffeeName + ", " + supplierID + ", " + price +
                        ", " + sales + ", " + total);

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
