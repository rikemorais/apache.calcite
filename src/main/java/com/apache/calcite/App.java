package com.apache.calcite;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class App {
  public static void main(String[] args) throws Exception {
    new App().run();
  }

  public void run() throws ClassNotFoundException, SQLException {
    Class.forName("org.apache.calcite.jdbc.Driver");
    Connection connection =
        DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    rootSchema.add("hr", new ReflectiveSchema(new Hr()));
    rootSchema.add("food", new ReflectiveSchema(new Food()));
    Statement statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery("select *\n"
            + "from \"food\".\"sales_fact_1997\" as s\n"
            + "join \"hr\".\"employees\" as e\n"
            + "on e.\"employeesId\" = s.\"customerId\"");
    final StringBuilder buf = new StringBuilder();
    while (resultSet.next()) {
      int n = resultSet.getMetaData().getColumnCount();
      for (int i = 1; i <= n; i++) {
        buf.append(i > 1 ? "; " : "")
            .append(resultSet.getMetaData().getColumnLabel(i))
            .append("=")
            .append(resultSet.getObject(i));
      }
      System.out.println(buf.toString());
      buf.setLength(0);
    }
    resultSet.close();
    statement.close();
    connection.close();
  }

  /** Object that will be used via reflection to create the "hr" schema. */
  public static class Hr {
    public final Employee[] employees = {
        new Employee(100, "Bill"),
        new Employee(200, "Eric"),
        new Employee(150, "Sebastian"),
    };
  }

  /** Object that will be used via reflection to create the "employees" table. */
  public static class Employee {
    public final int employeesId;
    public final String name;

    public Employee(int employeesId, String name) {
      this.employeesId = employeesId;
      this.name = name;
    }
  }

  // Object that will be used via reflection to create the "food" schema
  public static class Food {
    public final SalesFact[] sales_fact_1997 = {
        new SalesFact(100, 10),
        new SalesFact(150, 20),
    };
  }


  /** Object that will be used via reflection to create the
   * "sales_fact_1997" fact table. */
  public static class SalesFact {
    public final int customerId;
    public final int productId;

    public SalesFact(int customerId, int productId) {
      this.customerId = customerId;
      this.productId = productId;
    }
  }
}