package by.yaroslav_yakovlev;

import java.sql.*;

public class Main {
    private static final String PROTOCOL = "jdbc:postgresql://";
    private static final String DRIVER = "org.postgresql.Driver";
    private static final String URL_LOCALE_NAME = "host.docker.internal/";

    private static final String DATABASE_NAME = "Corporation";

    public static final String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;
    public static final String USER_NAME = "yaroslav";
    public static final String DATABASE_PASS = "postgresmaster";

    public static void main(String[] args) {
        checkDriver();
        checkDB();
        System.out.println("Подключение к базе данных | " + DATABASE_URL + "\n");
        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {
//            addEmployee(connection, "Андрей", "Борисов", "Анатольевич",
//                    Date.valueOf("1997-10-12"), "6544733722", 256000, 1, 1);
//            getEmployeesWhereSalaryMore100_000(connection);
//            getEmployeesWhereDepartmentIsN_PostIsM(connection);
//            correctStatusProjectNToStatusM(connection, 1, 2);
//            correctEmployees_WithProjectN_WherePostIsM_ToProjectK(connection, 1, 2, 3);
//            correctSalaryWhereEmployment_dateMore3Years(connection);
//            getDepartmentWhereMaxEmployees(connection);
//            removeEmployeesWhereRatingUnder1(connection, 3);
            getAvgSalaryWhereAgeMore30(connection);
//            removeEmployee(connection, 3);
        } catch (SQLException e) {
            if (e.getSQLState().startsWith("23")) {
                System.out.println("Произошло дублирование данных");
            } else throw new RuntimeException(e);
        }
    }

    public static void checkDriver () {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Нет JDBC-драйвера! Подключите JDBC-драйвер к проекту согласно инструкции.");
            throw new RuntimeException(e);
        }
    }

    public static void checkDB () {
        try {
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS);
        } catch (SQLException e) {
            System.out.println("Нет базы данных! Проверьте имя базы, путь к базе или разверните локально резервную копию согласно инструкции");
            throw new RuntimeException(e);
        }
    }

    private static void addEmployee(Connection connection, String first_name, String last_name,
                                    String patronymic, Date birth_date, String num_passport, int salary,
                                    Date employment_date, int rating, int post_id, int project_id) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO employees(first_name, last_name, birth_date, patronymic, " +
                "num_passport, salary, employment_date, rating, post_id, project_id) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) returning id;", Statement.RETURN_GENERATED_KEYS);
        statement.setString(1, first_name);
        statement.setString(2, last_name);
        statement.setString(3, patronymic);
        statement.setDate(4, birth_date);
        statement.setString(5, num_passport);
        statement.setInt(6, salary);
        statement.setDate(7, employment_date);
        statement.setInt(8, rating);
        statement.setInt(9, post_id);
        statement.setInt(10, project_id);
    }

    private static void addEmployee(Connection connection, String first_name, String last_name,
                                    String patronymic, Date birth_date, String num_passport, int salary,
                                    int post_id, int project_id) throws SQLException {

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO employees(first_name, last_name, patronymic, birth_date, " +
                        "num_passport, salary, post_id, project_id) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?) returning id;", Statement.RETURN_GENERATED_KEYS);
        statement.setString(1, first_name);
        statement.setString(2, last_name);
        statement.setString(3, patronymic);
        statement.setDate(4, birth_date);
        statement.setString(5, num_passport);
        statement.setInt(6, salary);
        statement.setInt(7, post_id);
        statement.setInt(8, project_id);

        int count = statement.executeUpdate();
        System.out.println("INSERT " + count + " strings");
    }

    private static void removeEmployee(Connection connection, int id) throws SQLException {
        PreparedStatement statement = connection.prepareStatement("DELETE FROM employees WHERE id = ?");
        statement.setInt(1, id);
        int count = statement.executeUpdate();
        System.out.println("DELETE " + count + " strings");
    }

    private static void getEmployeesWhereSalaryMore100_000(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM employees WHERE salary > 100_000;");

        while (rs.next()) {  // пока есть данные
            int param0 = rs.getInt(1);
            String param1 = rs.getString(2);
            String param2 = rs.getString(3);
            String param3 = rs.getString(4);
            Date param4 = rs.getDate(5);
            String param5 = rs.getString(6);
            int param6 = rs.getInt(7);
            Date param7 = rs.getDate(8);
            int param8 = rs.getInt(9);
            int param9 = rs.getInt(10);
            int param10 = rs.getInt(11);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " +
                    param3 + " | " + param4 + " | " + param5 + " | " +
                    param6 + " | " + param7 + " | " + param8 + " | " +
                    param9 + " | " + param10);
        }
    }

    private static void getEmployeesWhereDepartmentIsN_PostIsM(Connection connection) throws SQLException {
        /* Получим всех сотрудников с должностью Senior backend разработчик(id = 1) которые работают
        в отделе производства и разработки(id = 1) */
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT employees.id, first_name, last_name, patronymic, " +
                "(SELECT name FROM posts WHERE posts.id = post_id), " +
                "departments.name AS department FROM employees, departments WHERE \n" +
                "post_id = 1 and departments.id = " +
                "(SELECT department_id FROM posts WHERE posts.id = post_id LIMIT 1);");

        int param0 = -1;
        String param1, param2, param3 = null, param4 = null, param5 = null;

        while (rs.next()) {  // пока есть данные
            param0 = rs.getInt(1);
            param1 = rs.getString(2);
            param2 = rs.getString(3);
            param3 = rs.getString(4);
            param4 = rs.getString(5);
            param5 = rs.getString(6);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " +
                    param3 + " | " + param4 + " | " + param5);
        }
    }

    private static void correctStatusProjectNToStatusM(Connection connection, int id, int status_id) throws SQLException {
        // Изменим статус проекту под id = 1 на статус "На этапе тестирования"(id=2)
        PreparedStatement statement =connection.prepareStatement("UPDATE projects SET status_id = ? " +
                "WHERE id = ?;");
        statement.setInt(1, status_id);
        statement.setInt(2, id);

        int count = statement.executeUpdate();
        System.out.println("UPDATE " + count + " strings");
    }

    private static void correctEmployees_WithProjectN_WherePostIsM_ToProjectK(Connection connection, int project_id, int post_id, int new_project_id) throws SQLException {
        PreparedStatement statement =connection.prepareStatement("UPDATE employees SET project_id = ? " +
                "WHERE post_id = ? AND project_id = ?;");
        statement.setInt(1, new_project_id);
        statement.setInt(2, post_id);
        statement.setInt(3, project_id);

        int count = statement.executeUpdate();
        System.out.println("UPDATE " + count + " strings");
    }

    private static void correctSalaryWhereEmployment_dateMore3Years(Connection connection) throws SQLException {
        PreparedStatement statement =connection.prepareStatement("UPDATE employees SET salary = salary * 1.3 " +
                "WHERE current_date - employment_date >= 365 * 3;");

        int count = statement.executeUpdate();
        System.out.println("UPDATE " + count + " strings");
    }

    private static void getDepartmentWhereMaxEmployees(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT id, name FROM " +
                "(SELECT d.id, d.name, COUNT(e.id) " +
                "FROM employees e JOIN posts p ON e.post_id = p.id " +
                "JOIN departments d ON p.department_id = d.id GROUP BY d.id) " +
                "ORDER BY count DESC LIMIT 1;");

        while (rs.next()) {  // пока есть данные
            int param0 = rs.getInt(1);
            String param1 = rs.getString(2);
            System.out.println(param0 + " | " + param1);
        }

    }

    private static void removeEmployeesWhereRatingUnderN(Connection connection, int rating) throws SQLException {
        PreparedStatement statement = connection.prepareStatement("DELETE from employees WHERE rating < ?");
        statement.setInt(1, rating);
        int count = statement.executeUpdate();
        System.out.println("DELETE " + count + " strings");
    }

    private static void getAvgSalaryWhereAgeMore30(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT AVG(salary) FROM employees " +
                "WHERE current_date - birth_date > 30 * 365;");
        if (rs.next()) System.out.println(rs.getInt(1));
    }
}
