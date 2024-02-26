package protodb;

import protodb.dbengine.ProtoDB;
import protodb.dbengine.parser.CreateTableData;
import protodb.dbengine.parser.Parser;
import protodb.dbengine.plan.Plan;
import protodb.dbengine.query.Scan;
import protodb.dbengine.record.Schema;
import protodb.dbengine.xact.Transaction;

import java.io.File;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        // Press Opt+Enter with your caret at the highlighted text to see how
        // IntelliJ IDEA suggests fixing it.
        System.out.printf("Hello and welcome!");

        File dbDirectory = new File("testing");


        if (dbDirectory.exists()) {
            System.out.println("The directory exists.");
        } else {
            System.out.println("The directory does not exist.");
        }
        ProtoDB db = new ProtoDB("testing");
        Scanner scanner = new Scanner(System.in);

        System.out.println("Command Launcher started. Type 'exit' to quit.");

        while (true) {
            System.out.print("Enter command: ");
            String command = scanner.nextLine(); // Read user input

            if ("exit".equalsIgnoreCase(command)) {
                System.out.println("Exiting Command Launcher...");
                break; // Exit the loop (and thus the program) if user types "exit"
            }
            processCommand(command);
            execute(db, command);
        }

        scanner.close(); // Close the scanner
    }

    private static void execute(ProtoDB db, String q) {
        Transaction curr = db.newTx();
        if (q.startsWith("create")) {
            db.planner().executeUpdate(q, curr);
        } else if (q.startsWith("insert")) {
            db.planner().executeUpdate(q, curr);
        } else {
            Plan p = db.planner().createQueryPlan(q, curr);
            Scan s = p.open();
            Schema schema = p.schema();
            s.beforeFirst();
            while (s.next()) {
                String row = "";
                for (String field: schema.fields()) {
                    row += s.getVal(field).toString();
                    row += " ";
                }
                System.out.println(row);
            }
        }
        curr.commit();
    }


    private static void processCommand(String command) {
        // Placeholder for command processing logic
        System.out.println("Processing command: " + command);
    }

}