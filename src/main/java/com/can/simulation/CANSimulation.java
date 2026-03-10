package com.can.simulation;

import com.can.core.CANNetwork;
import com.can.core.CANNode;
import com.can.util.HashUtil;

import java.util.*;
import java.util.logging.*;
import java.util.stream.Collectors;


public class CANSimulation {

    static {
        Logger root = Logger.getLogger("");
        root.setLevel(Level.WARNING);
        for (Handler h : root.getHandlers()) {
            h.setFormatter(new SimpleFormatter() {
                @Override
                public String format(LogRecord r) {
                    return "[" + r.getLevel() + "] " + r.getMessage() + "\n";
                }
            });
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Scanner sc = new Scanner(System.in);

        System.out.println("=== CAN Interactive Simulation ===\n");

        int dims = promptInt(sc,
            "Enter number of dimensions for the keyspace (e.g. 2 or 3): ", 1, 8);

        System.out.print("Enter a name for the bootstrap (root) node: ");
        String rootName = sc.nextLine().trim();
        if (rootName.isEmpty()) rootName = "root";

        CANNetwork net = new CANNetwork(dims);
        net.bootstrap(rootName);
        System.out.println("  Bootstrap node \"" + rootName + "\" is ready.\n");

        boolean running = true;
        while (running) {
            printMenu();
            System.out.print("Choice: ");
            String choice = sc.nextLine().trim();

            switch (choice) {

                // add single node 
                case "1": {
                    System.out.print("  Node name: ");
                    String name = sc.nextLine().trim();
                    if (name.isEmpty()) { System.out.println("  Name cannot be empty."); break; }
                    if (net.getNode(name) != null) { System.out.println("  \"" + name + "\" already exists."); break; }

                    String bootstrap = chooseBootstrapNode(sc, net);
                    if (bootstrap == null) break;

                    try {
                        CANNode n = net.addNode(name, bootstrap);
                        System.out.println("  Added \"" + name + "\"  zone=" + n.getZone());
                    } catch (Exception e) {
                        System.out.println("  Failed: " + e.getMessage());
                    }
                    break;
                }

                // add multiple nodes 
                case "2": {
                    System.out.print("  Node names (comma-separated): ");
                    String line = sc.nextLine().trim();
                    if (line.isEmpty()) break;

                    String bootstrap = chooseBootstrapNode(sc, net);
                    if (bootstrap == null) break;

                    for (String raw : line.split(",")) {
                        String name = raw.trim();
                        if (name.isEmpty()) continue;
                        if (net.getNode(name) != null) {
                            System.out.println("  Skipping \"" + name + "\" — already exists.");
                            continue;
                        }
                        try {
                            CANNode n = net.addNode(name, bootstrap);
                            System.out.println("  Added \"" + name + "\"  zone=" + n.getZone());
                        } catch (Exception e) {
                            System.out.println("  Failed to add \"" + name + "\": " + e.getMessage());
                        }
                    }
                    break;
                }

                // remove node 
                case "3": {
                    if (net.size() <= 1) { System.out.println("  Cannot remove the last node."); break; }
                    System.out.print("  Node name to remove: ");
                    String name = sc.nextLine().trim();
                    CANNode node = net.getNode(name);
                    if (node == null) { System.out.println("  \"" + name + "\" not found."); break; }
                    System.out.println("  \"" + name + "\" zone before leave: " + node.getZone());
                    node.leave();
                    Thread.sleep(100);
                    System.out.println("  Done. Network size now: " + net.size());
                    break;
                }

                // store single 
                case "4": {
                    System.out.print("  Key  : ");
                    String key = sc.nextLine().trim();
                    System.out.print("  Value: ");
                    String value = sc.nextLine().trim();
                    if (key.isEmpty()) { System.out.println("  Key cannot be empty."); break; }
                    boolean ok = net.store(key, value);
                    String owner = net.responsibleNode(key).map(CANNode::getNodeId).orElse("??");
                    System.out.printf("  %-24s -> %-10s  %s%n", key, owner, ok ? "OK" : "FAILED");
                    break;
                }

                // lookup single 
                case "5": {
                    System.out.print("  Key: ");
                    String key = sc.nextLine().trim();
                    if (key.isEmpty()) { System.out.println("  Key cannot be empty."); break; }
                    Optional<String> result = net.lookup(key);
                    if (result.isPresent()) System.out.println("  HIT  \"" + result.get() + "\"");
                    else                   System.out.println("  miss — key not found.");
                    break;
                }

                // store multiple 
                case "6": {
                    System.out.println("  Enter key=value pairs, one per line. Blank line to finish.");
                    while (true) {
                        System.out.print("  > ");
                        String line = sc.nextLine().trim();
                        if (line.isEmpty()) break;
                        int eq = line.indexOf('=');
                        if (eq < 1) { System.out.println("  Format: key=value"); continue; }
                        String key = line.substring(0, eq).trim();
                        String val = line.substring(eq + 1).trim();
                        boolean ok = net.store(key, val);
                        String owner = net.responsibleNode(key).map(CANNode::getNodeId).orElse("??");
                        System.out.printf("  %-24s -> %-10s  %s%n", key, owner, ok ? "ok" : "FAILED");
                    }
                    break;
                }

                // lookup multiple 
                case "7": {
                    System.out.println("  Enter keys to look up, one per line. Blank line to finish.");
                    while (true) {
                        System.out.print("  > ");
                        String key = sc.nextLine().trim();
                        if (key.isEmpty()) break;
                        Optional<String> result = net.lookup(key);
                        if (result.isPresent())
                            System.out.printf("  HIT  %-24s  \"%s\"%n", key, result.get());
                        else
                            System.out.printf("  miss %-24s%n", key);
                    }
                    break;
                }

                // summary 
                case "8":  net.printSummary(); break;

                // partitioning 
                case "9": {
                    System.out.print("Checking partitioning... ");
                    boolean valid = net.verifyPartitioning();
                    System.out.println(valid ? "OK" : "FAILED — check zone splits!");
                    break;
                }

                // metrics 
                case "10": {
                    System.out.println("Node metrics:");
                    net.getAllNodes().stream()
                        .sorted(Comparator.comparing(CANNode::getNodeId))
                        .forEach(n -> System.out.println("  " + n.metricsString()));
                    System.out.println();
                    break;
                }
                // hash mapping 
                case "11": {
                    System.out.println("  Enter keys to hash, one per line. Blank line to finish.");
                    while (true) {
                        System.out.print("  > ");
                        String key = sc.nextLine().trim();
                        if (key.isEmpty()) break;
                        String owner = net.responsibleNode(key).map(CANNode::getNodeId).orElse("(none)");
                        System.out.printf("  %-24s  sha256=%-18s  -> %-20s  node=%-12s%n", key, HashUtil.sha256Hex(key).substring(0, 16) + "...", HashUtil.hashKey(key, dims), owner);
                    }
                    break;
                }

                // list nodes 
                case "12": {
                    System.out.println("  Current nodes (" + net.size() + "):");
                    net.getAllNodes().stream()
                        .sorted(Comparator.comparing(CANNode::getNodeId))
                        .forEach(n -> System.out.printf(
                            "  %-14s  zone=%-30s  nbrs=%d  entries=%d%n", n.getNodeId(), n.getZone(), n.getRoutingTable().size(), n.getDataStore().size()));
                    System.out.println();
                    break;
                }

                // quit
                case "0": {
                    System.out.println("Goodbye.");
                    running = false;
                    break;
                }

                default:
                    System.out.println("  Unknown option — try again.");
            }
        }
    }
    // helpers

    private static void printMenu() {
        System.out.println();
        System.out.println("          CAN Simulation Menu");
        System.out.println("  Node management");
        System.out.println("   1  Add a single node");
        System.out.println("   2  Add multiple nodes (comma list)");
        System.out.println("   3  Remove (graceful leave) a node");
        System.out.println("  Data operations");
        System.out.println("   4  Store a key/value pair");
        System.out.println("   5  Look up a key");
        System.out.println("   6  Store multiple key/value pairs");
        System.out.println("   7  Look up multiple keys");
        System.out.println("  Diagnostics");
        System.out.println("   8  Print network summary");
        System.out.println("   9  Verify zone partitioning");
        System.out.println("  10  Show per-node metrics");
        System.out.println("  11  Show key → coordinate mapping");
        System.out.println("  12  List all nodes");
        System.out.println("   0  Quit");
    }

    private static String chooseBootstrapNode(Scanner sc, CANNetwork net) {
        if (net.size() == 0) {
            System.out.println("  Network is empty.");
            return null;
        }
        if (net.size() == 1) {
            String only = net.getAllNodes().get(0).getNodeId();
            System.out.println("  (bootstrap contact auto-selected: " + only + ")");
            return only;
        }

        // build sorted list so numbers are stable and predictable
        List<CANNode> sorted = net.getAllNodes().stream()
            .sorted(Comparator.comparing(CANNode::getNodeId))
            .collect(Collectors.toList());

        System.out.println("  Available bootstrap contacts:");
        for (int i = 0; i < sorted.size(); i++)
            System.out.printf("    [%d] %s%n", i + 1, sorted.get(i).getNodeId());

        System.out.print("  Enter number or name: ");
        String input = sc.nextLine().trim();

        // try numeric index first
        try {
            int idx = Integer.parseInt(input) - 1;          // 1-based → 0-based
            if (idx >= 0 && idx < sorted.size())
                return sorted.get(idx).getNodeId();
            System.out.println("  Number out of range — operation cancelled.");
            return null;
        } catch (NumberFormatException ignored) {
            // not a number — fall through to name lookup
        }

        // try exact name match
        if (net.getNode(input) != null)
            return input;

        System.out.println("  \"" + input + "\" not found — operation cancelled.");
        return null;
    }

    private static int promptInt(Scanner sc, String prompt, int min, int max) {
        while (true) {
            System.out.print(prompt);
            try {
                int v = Integer.parseInt(sc.nextLine().trim());
                if (v >= min && v <= max) return v;
                System.out.println("  Please enter a value between " + min + " and " + max + ".");
            } catch (NumberFormatException e) {
                System.out.println("  Not a valid integer — try again.");
            }
        }
    }
}