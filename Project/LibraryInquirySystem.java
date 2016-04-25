import java.io.*;
import java.sql.*;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;

public class LibraryInquirySystem {
    private Connection conn;

    public void launch(){
        String jdbcName = "oracle.jdbc.driver.OracleDriver"; //String jdbcName = "oracle.jdbc.driver.OracleDriver";
        String link = "jdbc:oracle:thin:@db12.cse.cuhk.edu.hk:1521:db12"; //String link = "jdbc:oracle:thin:@db12.cse.cuhk.edu.hk:1521:db12";
        String username = "c004";
        String password = "riecdylb";
        try{
            Class.forName(jdbcName);
            conn = DriverManager.getConnection(link, username, password);
        }catch(Exception e){
            e.printStackTrace();
            System.out.println("Failed to connect to database");
            return;
        }
        System.out.println("Welcome to library inquiry system!");
        while(displayMainMenu());
        try{
            conn.close();
        }catch(Exception e){
            e.printStackTrace();
            System.out.println("Failed to close database");
        }
    }

    private BufferedReader readFile(String folder, String fileName){
        BufferedReader in;
        try{
            File target = new File(folder+"/"+fileName);
            in = new BufferedReader(new FileReader(target));

        }catch(Exception e){
            System.out.println("Unable to load '"+folder+"/"+fileName+"'");
            return null;
        }
        return in;
    }

    private List<String[]> getStrData(String folder, String fileName) {
        BufferedReader in = readFile(folder, fileName);
        if(in == null){
            return null;
        }
        List<String[]> data = new ArrayList<String[]>();
        try{
            String line = in.readLine();
            while(line != null) {
                if (line.length() > 0) {
                    data.add(line.split("\t"));
                }
                line = in.readLine();
            }
        }catch(Exception e){
            System.out.println("Invalid format: '"+folder+"/"+fileName+"'");
            return null;
        }
        return data;
    }

    private String getStrInput(BufferedReader in, String question){
        String result = "";
        while(result.length() == 0){
            System.out.print(question);
            try{
                result = in.readLine();
            }catch(Exception e){
                System.out.println("Invalid input, please try again");
            }
        }
        return result;
    }

    private int getIntInput(BufferedReader in, String question, int[] range){
        int result = -1;
        while(result < 0){
            System.out.print(question);
            try {
                String input = in.readLine();
                result = Integer.parseInt(input);
                if(result < range[0] || result > range[1]){
                    System.out.println("Invalid input, please try again");
                    result = -1;
                }
            }catch(Exception e){
                System.out.println("Invalid input, please try again");
            }
        }
        return result;
    }

    private Date getDateInput(BufferedReader in, String question){
        Date result = null;
        SimpleDateFormat df = new SimpleDateFormat("dd/MM/yyyy");
        while(result == null){
            System.out.print(question);
            try {
                String input = in.readLine();
                result = new java.sql.Date(df.parse(input).getTime());
            }catch(Exception e){
                System.out.println("Invalid input, please try again");
            }
        }
        return result;
    }

    private String getUserID(BufferedReader in){
        String user = getStrInput(in, "Enter the User ID: ");
        try{
            PreparedStatement pstmt = conn.prepareStatement("SELECT COUNT(*) FROM Libuser where libuid = ?");
            pstmt.setString(1, user);
            ResultSet rs1 = pstmt.executeQuery();
            rs1.next();
            if(rs1.getInt(1) == 0){
                System.out.println("[Error]: No such user");
                return null;
            }
            pstmt.close();
        }catch(Exception e){
            System.out.println("[Error] Unable to perform operation");
            return null;
        }
        return user;
    }

    private String getAuthors(String callnum){
        String authors;
        try{
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT aname FROM Authorship WHERE callnum = '"+callnum+"'");
            if(!rs.next()){
                return "";
            }
            authors = rs.getString(1);
            while(rs.next()){
                authors += ", "+rs.getString(1);
            }
            stmt.close();
        }catch(Exception e){
            return "";
        }
        return authors;
    }

    private String getTitle(String callnum){
        String title;
        try{
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT title FROM Book WHERE callnum = '"+callnum+"'");
            if(!rs.next()){
                return "";
            }
            title = rs.getString(1);
            stmt.close();
        }catch(Exception e){
            return "";
        }
        return title;
    }

    private int displayMenu(String menuName, String[] options){
        System.out.println("\n-----"+menuName+"-----\nWhat kinds of operation would you like to perform?");
        for(int i=0;i<options.length; i++){
            System.out.println((i+1)+". "+options[i]);
        }
        BufferedReader in = new BufferedReader (new InputStreamReader(System.in));
        return getIntInput(in, "Enter Your Choice: ", new int[]{1, options.length});
    }

    private boolean displayMainMenu(){
        String[] options = {"Operations for administrator", "Operations for library user", "Operations for librarian", "Operations for library director", "Exit this program"};
        int choice = displayMenu("Main menu", options);
        switch(choice){
            case 1:
                while(displayAdminMenu());
                break;
            case 2:
                while(displayUserMenu());
                break;
            case 3:
                while(displayLibrarianMenu());
                break;
            case 4:
                while(displayDirectorMenu());
                break;
            case 5:
                return false;
        }
        return true;
    }

    private boolean displayAdminMenu(){
        String[] options = {"Create all tables", "Delete all tables", "Load from datafile", "Show number of records in each table", "Return to the main menu"};
        int choice = displayMenu("Operations for administrator menu", options);
        switch(choice){
            case 1:
                createTables();
                break;
            case 2:
                dropTables();
                break;
            case 3:
                loadData();
                break;
            case 4:
                countRecords();
                break;
            case 5:
                return false;
        }
        return true;
    }

    private void createTables(){
        System.out.print("Processing...");
        try{
            Statement stmt = conn.createStatement();
            String table1 = "CREATE TABLE Category (cid INT NOT NULL, max INT NOT NULL, period INT NOT NULL, PRIMARY KEY(cid))";
            String table2 = "CREATE TABLE Libuser (libuid VARCHAR(10) NOT NULL, name VARCHAR(25) NOT NULL, address VARCHAR(100) NOT NULL, cid INT NOT NULL, PRIMARY KEY(libuid))";
            String table3 = "CREATE TABLE Book (callnum VARCHAR(8) NOT NULL, title VARCHAR(30) NOT NULL, publish DATE NOT NULL, PRIMARY KEY(callnum))";
            String table4 = "CREATE TABLE Copy (callnum VARCHAR(8) NOT NULL, copynum INT NOT NULL, PRIMARY KEY(callnum, copynum), FOREIGN KEY (callnum) REFERENCES Book(callnum))";
            String table5 = "CREATE TABLE Borrow (libuid VARCHAR(10) NOT NULL, callnum VARCHAR(8) NOT NULL, copynum INT NOT NULL, checkout DATE NOT NULL, returndate DATE, PRIMARY KEY(libuid, callnum, copynum, checkout), FOREIGN KEY (libuid) REFERENCES Libuser(libuid), FOREIGN KEY (callnum, copynum) REFERENCES Copy(callnum, copynum))";
            String table6 = "CREATE TABLE Authorship (aname VARCHAR(25) NOT NULL, callnum VARCHAR(8) NOT NULL, PRIMARY KEY(aname, callnum), FOREIGN KEY (callnum) REFERENCES Book(callnum))";
            stmt.executeUpdate(table1);
            stmt.executeUpdate(table2);
            stmt.executeUpdate(table3);
            stmt.executeUpdate(table4);
            stmt.executeUpdate(table5);
            stmt.executeUpdate(table6);
            stmt.close();
        }catch(Exception e){
            System.out.println("\n[Error]: Failed to create tables");
            return;
        }
        System.out.println("Done! Database is initialized!");
    }

    private void dropTables(){
        System.out.print("Processing...");
        try {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("drop table Authorship");
			stmt.executeUpdate("drop table Borrow");
            stmt.executeUpdate("drop table Copy");
            stmt.executeUpdate("drop table Book");
			stmt.executeUpdate("drop table Libuser");
            stmt.executeUpdate("drop table Category");
        }catch(Exception e){
            System.out.println("\n[Error]: Failed to drop tables");
            return;
        }
        System.out.println("Done! Database is removed!");
    }

    private void loadData(){
        System.out.print("Type in the Source Data Folder Path: ");
        BufferedReader in = new BufferedReader (new InputStreamReader(System.in));
        try{
            String folder = in.readLine();
            System.out.print("Processing...");
            List<String[]> category= getStrData(folder, "category.txt");
            List<String[]> user= getStrData(folder, "user.txt");
            List<String[]> book= getStrData(folder, "book.txt");
            List<String[]> checkOut= getStrData(folder, "check_out.txt");
            if(category == null || user == null || book == null || checkOut == null){
                System.out.println("\n[Error]: Wrong path or Missing file");
                return;
            }
            PreparedStatement pstmt, pstmt2, pstmt3;
            pstmt = conn.prepareStatement("INSERT INTO Category VALUES (?, ?, ?)");
            for(String[] line : category){
                pstmt.setInt(1, Integer.parseInt(line[0]));
                pstmt.setInt(2, Integer.parseInt(line[1]));
                pstmt.setInt(3, Integer.parseInt(line[2]));
                pstmt.executeUpdate();
            }
            pstmt.close();
            pstmt = conn.prepareStatement("INSERT INTO Libuser VALUES (?, ?, ?, ?)");
            for(String[] line : user){
                pstmt.setString(1, line[0]);
                pstmt.setString(2, line[1]);
                pstmt.setString(3, line[2]);
                pstmt.setInt(4, Integer.parseInt(line[3]));
                pstmt.executeUpdate();
            }
            pstmt.close();
            SimpleDateFormat df = new SimpleDateFormat("dd/MM/yyyy");
            pstmt = conn.prepareStatement("INSERT INTO Book VALUES (?, ?, ?)");
            pstmt2 = conn.prepareStatement("INSERT INTO Copy VALUES (?, ?)");
            pstmt3 = conn.prepareStatement("INSERT INTO Authorship VALUES (?, ?)");
            for(String[] line : book){
                pstmt.setString(1, line[0]);
                pstmt.setString(2, line[2]);
                pstmt.setDate(3, new java.sql.Date(df.parse(line[4]).getTime()));
                pstmt.executeUpdate();
                int copies = Integer.parseInt(line[1]);
                for(int j=0; j<copies; j++) {
                    pstmt2.setString(1, line[0]);
                    pstmt2.setInt(2, j + 1);
                    pstmt2.executeUpdate();
                }
                String[] authors = line[3].split(",");
                for(String name : authors){
                    pstmt3.setString(1, name);
                    pstmt3.setString(2, line[0]);
                    pstmt3.executeUpdate();
                }
            }
            pstmt.close();
            pstmt2.close();
            pstmt3.close();
            pstmt = conn.prepareStatement("INSERT INTO Borrow VALUES (?, ?, ?, ?, ?)");
            for(String[] line : checkOut){
                if(!line[4].equals("null")){
                    pstmt.setDate(5, new java.sql.Date(df.parse(line[4]).getTime()));
                }else{
                    pstmt.setDate(5, null);
                }
                pstmt.setString(1, line[2]);
                pstmt.setString(2, line[0]);
                pstmt.setInt(3, Integer.parseInt(line[1]));
                pstmt.setDate(4, new java.sql.Date(df.parse(line[3]).getTime()));
                pstmt.executeUpdate();
            }
            pstmt.close();
        }catch(Exception e){
            System.out.println("\n[Error]: Invalid format");
            return;
        }
        System.out.println("Done! Data is inputted to the database!");
    }

    private void countRecords(){
        String[] tables = {"Authorship", "Book", "Borrow", "Category", "Copy", "Libuser"};
        try{
            Statement stmt = conn.createStatement();
            for(String table : tables){
                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM "+table);
                rs.next();
                int count = rs.getInt(1);
                System.out.println(table+": "+count);
            }
        }catch(Exception e){
            System.out.println("[Error]: Failed to retrieve all tables");
        }
    }

    private boolean displayUserMenu(){
        String[] options = {"Search for Books", "Show loan record of a user", "Return to the main menu"};
        int choice = displayMenu("Operations for library user menu", options);
        switch(choice){
            case 1:
                searchBooks();
                break;
            case 2:
                showLoanRecord();
                break;
            case 3:
                return false;
        }
        return true;
    }

    private void searchBooks(){
        System.out.println("Choose the search criterion:\n1. call number\n2. title\n3. author");
        BufferedReader in = new BufferedReader (new InputStreamReader(System.in));
        int type = getIntInput(in, "Choose the search criterion: ", new int[]{1, 3});
        String keyword = getStrInput(in, "Type in the Search Keyword: ");
        try{
            String query = "";
            String search = keyword;
            switch(type){
                case 1:
                    query = "SELECT callnum FROM Book WHERE callnum = ?";
                    break;
                case 2:
                    query = "SELECT callnum FROM Book WHERE title LIKE ? ORDER BY callnum";
                    search = "%"+keyword+"%";
                    break;
                case 3:
                    query = "SELECT DISTINCT callnum FROM Authorship WHERE aname LIKE ? ORDER BY callnum";
                    search = "%"+keyword+"%";
                    break;
            }
            PreparedStatement pstmt = conn.prepareStatement(query);
            pstmt.setString(1, search);
            ResultSet callnums = pstmt.executeQuery();
            List<String> books = new ArrayList<String>();
            while(callnums.next()){
                books.add(callnums.getString(1));
            }
            pstmt.close();
            if(books.size() == 0){
                System.out.println("[Error]: No match found");
                return;
            }
            System.out.println("|Call Num|Title|Author|Available No. of Copy|");
            Statement stmt = conn.createStatement();
            for(String callnum : books){
                String title = getTitle(callnum);
                String authors = getAuthors(callnum);
                ResultSet rs3 = stmt.executeQuery("SELECT COUNT(*) FROM Copy WHERE callnum = '"+callnum+"'");
                rs3.next();
                int total = rs3.getInt(1);
                ResultSet rs4 = stmt.executeQuery("SELECT COUNT(*) FROM Borrow WHERE callnum = '"+callnum+"'AND returndate IS NULL");
                rs4.next();
                int checkedOut = rs4.getInt(1);
                System.out.println("|"+callnum+"|"+title+"|"+authors+"|"+(total-checkedOut)+"|");
            }
            stmt.close();
            System.out.println("End of Query");
        }catch(Exception e){
            System.out.println("[Error]: Unable to make query");
        }
    }

    private void showLoanRecord(){
        BufferedReader in = new BufferedReader (new InputStreamReader(System.in));
        String user = getUserID(in);
        if(user == null){
            return;
        }
        try{
            System.out.println("Loan Record:\n|CallNum|CopyNum|Title|Author|Check-out|Returned?|");
            PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM Borrow where libuid = ?");
            pstmt.setString(1, user);
            ResultSet rs2 = pstmt.executeQuery();
            SimpleDateFormat df = new SimpleDateFormat("dd/MM/yyyy");
            while(rs2.next()){
                String callnum = rs2.getString(2);
                int copynum = rs2.getInt(3);
                String title = getTitle(callnum);
                String author = getAuthors(callnum);
                Date checkOut = rs2.getDate(4);
                boolean returned = (rs2.getDate(5)!=null);
                String record = "|"+callnum+"|"+copynum+"|"+title+"|"+author+"|"+df.format(checkOut)+"|";
                if(returned){
                    record += "Yes|";
                }else{
                    record += "No|";
                }
                System.out.println(record);
            }
            pstmt.close();
            System.out.println("End of Query");
        }catch(Exception e){
            System.out.println("[Error]: Unable to make query");
        }
    }

    private boolean displayLibrarianMenu(){
        String[] options = {"Book Borrowing", "Book Returning", "Return to the main menu"};
        int choice = displayMenu("Operations for librarian menu", options);
        switch(choice){
            case 1:
                borrowBook();
                break;
            case 2:
                returnBook();
                break;
            case 3:
                return false;
        }
        return true;
    }

    private void borrowBook(){
        BufferedReader in = new BufferedReader (new InputStreamReader(System.in));
        String user = getUserID(in);
        if(user == null){
            return;
        }
        String callnum = getStrInput(in, "Enter the Call Number: ");
        int copynum = getIntInput(in, "Enter the Copy Number: ", new int[]{1, 10});
        try{
            PreparedStatement pstmt = conn.prepareStatement("SELECT COUNT(*) FROM Copy WHERE callnum = ? AND copynum = ?");
            pstmt.setString(1, callnum);
            pstmt.setInt(2, copynum);
            ResultSet rs1 = pstmt.executeQuery();
            rs1.next();
            if(rs1.getInt(1) == 0){
                pstmt.close();
                System.out.println("[Error] No such book copy");
                return;
            }
            pstmt.close();
            pstmt = conn.prepareStatement("SELECT COUNT(*) FROM Borrow WHERE callnum = ? AND copynum = ? AND returndate IS NULL");
            pstmt.setString(1, callnum);
            pstmt.setInt(2, copynum);
            ResultSet rs2 = pstmt.executeQuery();
            rs2.next();
            if(rs2.getInt(1) > 0){
                pstmt.close();
                System.out.println("[Error] Book copy is currently checked out");
                return;
            }
            pstmt.close();
            pstmt = conn.prepareStatement("SELECT max FROM Category C, Libuser L WHERE C.cid = L.cid AND L.libuid = ?");
            pstmt.setString(1, user);
            ResultSet rs3 = pstmt.executeQuery();
            rs3.next();
            int max = rs3.getInt(1);
            pstmt.close();
            pstmt = conn.prepareStatement("SELECT COUNT(*) FROM Borrow WHERE libuid = ? AND returndate IS NULL");
            pstmt.setString(1, user);
            ResultSet rs4 = pstmt.executeQuery();
            rs4.next();
            if(rs4.getInt(1) >= max){
                pstmt.close();
                System.out.println("[Error] User has reached the checkout limit");
                return;
            }
            pstmt.close();
            Date today = new java.sql.Date(new java.util.Date().getTime());
            pstmt = conn.prepareStatement("INSERT INTO Borrow VALUES (?, ?, ?, ?, NULL)");
            pstmt.setString(1, user);
            pstmt.setString(2, callnum);
            pstmt.setInt(3, copynum);
            pstmt.setDate(4, today);
            pstmt.executeUpdate();
            pstmt.close();
            System.out.println("Book borrowing performed successfully!!!");
        }catch(Exception e){
            e.printStackTrace();
            System.out.println("[Error] Unable to perform operation");
        }
    }

    private void returnBook(){
        BufferedReader in = new BufferedReader (new InputStreamReader(System.in));
        String user = getUserID(in);
        if(user == null){
            return;
        }
        String callnum = getStrInput(in, "Enter the Call Number: ");
        int copynum = getIntInput(in, "Enter the Copy Number: ", new int[]{1, 10});
        try{
            PreparedStatement pstmt = conn.prepareStatement("SELECT checkout FROM Borrow WHERE libuid = ? AND callnum = ? AND copynum = ? AND returndate IS NULL");
            pstmt.setString(1, user);
            pstmt.setString(2, callnum);
            pstmt.setInt(3, copynum);
            ResultSet rs = pstmt.executeQuery();
            if(!rs.next()){
                System.out.println("[Error] An matching borrow record is not found.");
                return;
            }
            Date checkOut = rs.getDate(1);
            pstmt.close();
            Date today = new java.sql.Date(new java.util.Date().getTime());
            pstmt = conn.prepareStatement("UPDATE Borrow SET returndate = ? WHERE libuid = ? AND callnum = ? AND copynum = ? AND checkout = ?");
            pstmt.setDate(1, today);
            pstmt.setString(2, user);
            pstmt.setString(3, callnum);
            pstmt.setInt(4, copynum);
            pstmt.setDate(5, checkOut);
            pstmt.executeUpdate();
            pstmt.close();
            System.out.println("Book returning performed successfully!!!");
        }catch(Exception e){
            System.out.println("[Error] Unable to perform operation");
        }
    }

    private boolean displayDirectorMenu(){
        String[] options = {"List all un-returned book copies which are checked-out within a period", "Return to the main menu"};
        int choice = displayMenu("Operations for library director menu", options);
        switch(choice){
            case 1:
                listUnReturned();
                break;
            case 2:
                return false;
        }
        return true;
    }

    private void listUnReturned(){
        BufferedReader in = new BufferedReader (new InputStreamReader(System.in));
        Date startDate = getDateInput(in, "Type in the starting date [dd/mm/yyyy]: ");
        Date endDate = getDateInput(in, "Type in the ending date [dd/mm/yyyy]: ");
        SimpleDateFormat df = new SimpleDateFormat("dd/MM/yyyy");
        try{
            PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM Borrow WHERE checkout >= ? AND checkout <= ? AND returndate IS NULL ORDER BY checkout DESC");
            pstmt.setDate(1, startDate);
            pstmt.setDate(2, endDate);
            ResultSet rs = pstmt.executeQuery();
            System.out.println("List of UnReturned Book:\n|LibUID|CallNum|CopyNum|Checkout|");
            while(rs.next()){
                System.out.println("|"+rs.getString(1)+"|"+rs.getString(2)+"|"+rs.getString(3)+"|"+df.format(rs.getDate(4))+"|");
            }
            pstmt.close();
            System.out.println("End of Query");
        }catch(Exception e){
            System.out.println("[Error]: Unable to make query");
        }
    }

}
