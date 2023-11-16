import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class Controller {
    static private ConcurrentHashMap<String, IndexEntry> index; //States: "store in progress" "store complete" "remove in progress"
    static private CopyOnWriteArrayList<Integer> dstorePorts;
    static private ConcurrentHashMap<Integer, PrintWriter> dstoreWriters;
    static private ConcurrentHashMap<String, ArrayList<Integer>> storeAcks; // <filename, dstore ports>
    static private ConcurrentHashMap<String, ArrayList<String>> loadPorts; // <filename, dstore ports>
    static private ConcurrentHashMap<String, ArrayList<Integer>> removeAcks; // <filename, dstore ports>
    static private ConcurrentHashMap<Integer, ArrayList<String>> listResponses; // <dstore port, filenames>
    static private CopyOnWriteArrayList<Integer> listResponsePorts;
    static private CopyOnWriteArrayList<String> rebalanceAcks;
    static private AtomicInteger expectedRebalanceAckCount = new AtomicInteger();
    static private int cport; //controller's port number
    static private int r; //Replication factor
    static private int timeout; //Timeout length
    static private int rebalancePeriod; //Length of time until rebalance needed
    static private AtomicBoolean storingOrRemoving = new AtomicBoolean();
    static private AtomicBoolean rebalancing = new AtomicBoolean();
    static private CopyOnWriteArrayList<Integer> joinQueue;

    public static void main(String[] args) {
        cport = Integer.parseInt(args[0]);
        r = Integer.parseInt(args[1]);
        timeout = Integer.parseInt(args[2]);
        rebalancePeriod = Integer.parseInt(args[3]);
        index = new ConcurrentHashMap<>();
        dstorePorts = new CopyOnWriteArrayList<>();
        storeAcks = new ConcurrentHashMap<>();
        loadPorts = new ConcurrentHashMap<>();
        removeAcks = new ConcurrentHashMap<>();
        dstoreWriters = new ConcurrentHashMap<>();
        listResponses = new ConcurrentHashMap<>();
        listResponsePorts = new CopyOnWriteArrayList<>();
        storingOrRemoving.set(false);
        rebalancing.set(false);
        rebalanceAcks = new CopyOnWriteArrayList<>();
        expectedRebalanceAckCount.set(0);
        joinQueue = new CopyOnWriteArrayList<>();

        new Thread(new Controller() .new waitThenRebalanceThread()).start();
        startListening();
    }

    public static void startListening() {
        ServerSocket controllerSocket = null;
        try {
            controllerSocket = new ServerSocket(cport);
            while (true) {
                try {
                    Socket clientOrStore = controllerSocket.accept();
                    new Thread(new Controller() .new ConnectionThread(clientOrStore)).start();
                } catch(Exception e) { System.err.println("ERROR1: " + e); }
            }
        } catch(Exception e) { System.err.println("ERROR2: " + e);
        } finally {
            if (controllerSocket != null)
                try { controllerSocket.close(); } catch (IOException e) { System.err.println("ERROR: " + e); }
        }
    }

    public class ConnectionThread implements Runnable {
        enum ConnectionType {
            CLIENT,
            DSTORE,
            NOTSET
        }
        Socket clientOrStore;
        ConnectionType type = ConnectionType.NOTSET;
        boolean isFirstMessage = true;
        int dstorePort = -1;
        PrintWriter out = null;
        ConnectionThread(Socket socket) {
            clientOrStore=socket;
        }

        public void run() {
            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(clientOrStore.getInputStream()));
                out = new PrintWriter(clientOrStore.getOutputStream(), true);
                String line;
                while((line = in.readLine()) != null) {
                    System.out.println("{Received}: " + line);
                    String[] messageParts = line.split(" ");
                    //Check if first message to determine type
                    if (isFirstMessage) {
                        if (messageParts[0].equals(Protocol.JOIN_TOKEN)) {
                            type = ConnectionType.DSTORE;
                            dstorePort = Integer.parseInt(messageParts[1]);
                            //Add port to join queue
                            joinQueue.add(dstorePort);
                            //Wait until any rebalancing operation is complete, and first in queue
                            while (rebalancing.get() || joinQueue.get(0) != dstorePort) {}

                            dstorePorts.add(dstorePort);
                            System.out.println("Dstore added to list");
                            dstoreWriters.put(dstorePort, out);
                            //Start rebalance
                            while (rebalancing.get()) {}
                            new Thread(new rebalanceThread(false, dstorePort)).start();
                        } else {
                            type = ConnectionType.CLIENT;
                            evaluateClientMessage(messageParts);
                        }
                        isFirstMessage = false;
                    } else {
                        if (type == ConnectionType.CLIENT) {
                            evaluateClientMessage(messageParts);
                        } else if (type == ConnectionType.DSTORE) {
                            evaluateDstoreMessage(messageParts);
                        }
                    }
                }
                switch (type) {
                    case DSTORE -> System.out.println("Dstore " + dstorePort + " disconnected");
                    case CLIENT -> System.out.println("Client disconnected");
                    default -> System.out.println("Client/Dstore disconnected");
                }
            } catch(Exception e) {
                if (e instanceof SocketException) {
                    switch (type) {
                        case DSTORE -> System.out.println("Dstore " + dstorePort + " disconnected");
                        case CLIENT -> System.out.println("Client disconnected");
                        default -> System.out.println("Client/Dstore disconnected");
                    }
                } else {
                    System.err.println("ERROR: " + e);
                }
            } finally {
                try {clientOrStore.close();} catch(Exception e) {}
                if (type == ConnectionType.DSTORE && dstorePort != -1) {
                    dstorePorts.removeAll(Arrays.asList(dstorePort));
                    System.out.println("controller removed Dstore: " + dstorePort);
                    dstoreWriters.remove(dstorePort);
                    if (joinQueue.contains(dstorePort)) {
                        joinQueue.remove(dstorePort);
                    }
                }
            }
        }

        private void evaluateClientMessage(String[] messageParts) {
            while (rebalancing.get() || joinQueue.size() > 0) {} //Waits until any rebalancing operation is complete
            switch (messageParts[0]) {
                case Protocol.LIST_TOKEN: //LIST
                    if (dstorePorts.size() >= r) {
                        String sendString = Protocol.LIST_TOKEN + getFileList();
                        System.out.println("[Sent to client]: " + sendString);
                        out.println(sendString);
                    } else {
                        System.out.println("[Sent to client]: " + Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    }
                    break;
                case Protocol.STORE_TOKEN: //STORE filename filesize
                    if (dstorePorts.size() >= r) {
                        String filename = messageParts[1];
                        int filesize = Integer.parseInt(messageParts[2]);
                        if (!index.containsKey(filename)) {
                            storingOrRemoving.set(true);
                            index.put(filename, new IndexEntry(filesize, "store in progress", new ArrayList<>()));
                            if (storeAcks.containsKey(filename)) { storeAcks.remove(filename); }
                            storeAcks.put(filename, new ArrayList<>());
                            String portsString = getNOrderedDstorePorts(r);
                            if (portsString != "") {
                                //Start thread to look for all r acks from specified dstore ports
                                new Thread(new StoreAckThread(out, filename, portsString)).start();
                                System.out.println("[Sent to client]: " + Protocol.STORE_TO_TOKEN + portsString);
                                out.println(Protocol.STORE_TO_TOKEN + portsString);
                            } else {
                                System.out.println("[Sent to client]: " + Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                                out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                            }
                        } else {
                            System.out.println("[Sent to client]: " + Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                            out.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                        }
                    } else {
                        System.out.println("[Sent to client]: " + Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    }
                    break;
                case Protocol.LOAD_TOKEN: {
                    if (dstorePorts.size() >= r) {
                        String filename = messageParts[1];
                        if (index.containsKey(filename)) {
                            IndexEntry entry = index.get(filename);
                            if (entry.getState() == "store complete" && entry.getDstores().size() > 0) {
                                ArrayList<String> ports = new ArrayList<>();
                                //Make copy of ports list to avoid passing by reference
                                for (String s : entry.getDstores()) {
                                    ports.add(s);
                                }
                                String port1 = ports.get(0);
                                ports.remove(0);
                                //Remove existing entry in loadPorts
                                if (loadPorts.containsKey(filename)) {
                                    loadPorts.remove(filename);
                                }
                                loadPorts.put(filename, ports);
                                String filesize = String.valueOf(index.get(filename).getFileSize());
                                System.out.println("[Sent to client]: " + Protocol.LOAD_FROM_TOKEN + " " + port1 + " " + filesize);
                                out.println(Protocol.LOAD_FROM_TOKEN + " " + port1 + " " + filesize);
                            } else {
                                System.out.println("[Sent to client]: " + Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                                out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                            }
                        } else {
                            System.out.println("[Sent to client]: " + Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                            out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                        }
                    } else {
                        System.out.println("[Sent to client]: " + Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    }
                    break; }
                case Protocol.RELOAD_TOKEN: {
                    if (dstorePorts.size() >= r) {
                        String filename = messageParts[1];
                        if (index.containsKey(filename)) {
                            IndexEntry entry = index.get(filename);
                            if (entry.getState() == "store complete" && entry.getDstores().size() > 0 && loadPorts.containsKey(filename)) {
                                if (loadPorts.get(filename).size() > 0) {
                                    ArrayList<String> ports = loadPorts.get(filename);
                                    String port1 = ports.get(0);
                                    loadPorts.computeIfPresent(filename, (k,v) -> {
                                        v.remove(port1);
                                        return v;
                                    });
                                    String filesize = String.valueOf(index.get(filename).getFileSize());
                                    System.out.println("[Sent to client]: " + Protocol.LOAD_FROM_TOKEN + " " + port1 + " " + filesize);
                                    out.println(Protocol.LOAD_FROM_TOKEN + " " + port1 + " " + filesize);
                                } else {
                                    System.out.println("[Sent to client]: " + Protocol.ERROR_LOAD_TOKEN);
                                    out.println(Protocol.ERROR_LOAD_TOKEN);
                                }
                            } else {
                                System.out.println("[Sent to client]: " + Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                                out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                            }
                        } else {
                            System.out.println("[Sent to client]: " + Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                            out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                        }
                    } else {
                        System.out.println("[Sent to client]: " + Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    }
                    break; }
                case Protocol.REMOVE_TOKEN:
                    if (dstorePorts.size() >= r) {
                        String filename = messageParts[1];
                        if (index.containsKey(filename)) {
                            if (index.get(filename).getState().equals("store complete")) {
                                storingOrRemoving.set(true);
                                index.computeIfPresent(filename, (k,v) -> {
                                    v.setState("remove in progress");
                                    return v;
                                });
                                ArrayList<Integer> ports = new ArrayList<>();
                                for (String s : index.get(filename).getDstores()) {
                                    ports.add(Integer.parseInt(s));
                                }
                                if (removeAcks.containsKey(filename)) { removeAcks.remove(filename); }
                                removeAcks.put(filename, new ArrayList<>());
                                //Start thread to look for all r acks from specified dstore ports
                                new Thread(new RemoveAckThread(filename, ports, out)).start();
                                //send messages to dstores
                                for (Integer i : ports) {
                                    if (dstorePorts.contains(i)) {
                                        System.out.println("[Sent to dstore " + i + "]: " + Protocol.REMOVE_TOKEN + " " + filename);
                                        dstoreWriters.get(i).println(Protocol.REMOVE_TOKEN + " " + filename);
                                    } else {
                                        System.out.println("Dstore " + i + " not connected, no message sent");
                                    }
                                }
                            } else {
                                System.out.println("[Sent to client]: " + Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                                out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                            }
                        } else {
                            System.out.println("[Sent to client]: " + Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                            out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                        }
                    }else {
                        System.out.println("[Sent to client]: " + Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    }
                    break;
                default:
                    System.out.println("ERROR: controller received unrecognized client command: " + messageParts[0]);
                    break;
            }
        }

        private void evaluateDstoreMessage(String[] messageParts) {
            switch (messageParts[0]) {
                case Protocol.LIST_TOKEN:
                    ArrayList<String> filenames = new ArrayList<>();
                    for (int i = 1; i < messageParts.length; i++) {
                        filenames.add(messageParts[i]);
                    }
                    listResponses.put(dstorePort, filenames);
                    listResponsePorts.add(dstorePort);
                    break;
                case Protocol.JOIN_TOKEN:
                    System.out.println("ERROR: controller received JOIN twice");
                    break;
                case Protocol.STORE_ACK_TOKEN: {
                    String filename = messageParts[1];
                    if (storeAcks.containsKey(filename)) {
                        storeAcks.computeIfPresent(filename, (k,v) -> {
                            v.add(dstorePort);
                            return v;
                        });
                    }
                    break; }
                case Protocol.REMOVE_ACK_TOKEN: {
                    String filename = messageParts[1];
                    if (removeAcks.containsKey(filename)) {
                        removeAcks.computeIfPresent(filename, (k,v) -> {
                            v.add(dstorePort);
                            return v;
                        });
                    }
                    break; }
                case Protocol.REBALANCE_COMPLETE_TOKEN:
                    rebalanceAcks.add(String.valueOf(dstorePort));
                    break;
                case Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN:
                    //Leave file as remove in progress, future rebalances will ensure no dstores store this
                    break;
                default:
                    System.out.println("ERROR: controller received unrecognized dstore command: " + messageParts[0]);
                    break;
            }
        }

        class StoreAckThread implements Runnable {
            Socket clientOrStore = null;
            PrintWriter out = null;
            String file;
            ArrayList<Integer> dstorePorts = new ArrayList<>();
            public StoreAckThread(PrintWriter writer, String filename, String portsString) {
                out = writer;
                file = filename;
                String[] portsStringList = (portsString.split(" "));
                for (int i = 1; i < portsStringList.length; i++) {
                    dstorePorts.add(Integer.parseInt(portsStringList[i]));
                }
            }
            public void run() {
                ExecutorService executor = Executors.newSingleThreadExecutor();
                Future<String> future = executor.submit(new StoreAckTask(file, dstorePorts));
                try {
                    future.get(timeout, TimeUnit.MILLISECONDS);
                    //Received all acks
                    index.computeIfPresent(file, (k,v) -> {
                        v.setState("store complete");
                        for (Integer i : dstorePorts) {
                            v.addDstore(String.valueOf(i));
                        }
                        return v;
                    });
                    storeAcks.remove(file);
                    System.out.println("Received all store acks");
                    storingOrRemoving.set(false);
                    System.out.println("[Sent to client]: " + Protocol.STORE_COMPLETE_TOKEN);
                    out.println(Protocol.STORE_COMPLETE_TOKEN);
                } catch(Exception e) {
                    //Timed out
                    future.cancel(true);
                    index.remove(file);
                    storeAcks.remove(file);
                    storingOrRemoving.set(false);
                    System.out.println("Did not receive all store acks before timeout");
                }
                executor.shutdownNow();
            }
        }
        class StoreAckTask implements Callable<String>{
            String file;
            ArrayList<Integer> dstores = null;
            public StoreAckTask(String filename, ArrayList<Integer> ports) {
                file = filename;
                dstores = ports;
            }
            @Override
            public String call() {
                while (!storeAcks.get(file).containsAll(dstores)) {}
                return "received all acks";
            }
        }

        class RemoveAckThread implements Runnable {
            String file;
            ArrayList<Integer> dstorePorts = new ArrayList<>();
            PrintWriter out = null;
            public RemoveAckThread(String filename, ArrayList<Integer> ports, PrintWriter writer) {
                file = filename;
                dstorePorts = ports;
                out = writer;
            }
            public void run() {
                ExecutorService executor = Executors.newSingleThreadExecutor();
                Future<String> future = executor.submit(new RemoveAckTask(file, dstorePorts));
                try {
                    future.get(timeout, TimeUnit.MILLISECONDS);
                    //Received all acks
                    index.remove(file);
                    removeAcks.remove(file);
                    storingOrRemoving.set(false);
                    System.out.println("Received all remove acks");
                    System.out.println("[Sent to client]: " + Protocol.REMOVE_COMPLETE_TOKEN);
                    out.println(Protocol.REMOVE_COMPLETE_TOKEN);
                } catch(Exception e) {
                    //Timed out
                    future.cancel(true);
                    removeAcks.remove(file);
                    storingOrRemoving.set(false);
                    System.out.println("Did not receive all remove acks before timeout");
                }
                executor.shutdownNow();
            }
        }
        class RemoveAckTask implements Callable<String>{
            String file;
            ArrayList<Integer> dstores = null;
            public RemoveAckTask(String filename, ArrayList<Integer> ports) {
                file = filename;
                dstores = ports;
            }
            @Override
            public String call() {
                while (!removeAcks.get(file).containsAll(dstores)) {}
                return "Received all acks";
            }
        }
    }

    public class rebalanceThread implements  Runnable {
        boolean repeat;
        int dequeuePort;
        public rebalanceThread(boolean shouldRepeat, int portToDequeue) {
            repeat = shouldRepeat;
            dequeuePort = portToDequeue;
        }
        public void run() {
            //Wait until not storing or removing
            while (storingOrRemoving.get() || rebalancing.get()) {}
            outputIndex();
            rebalancing.set(true);
            System.out.println("Starting rebalance");
            rebalanceAcks.clear();
            expectedRebalanceAckCount.set(0);
            listResponses.clear();
            listResponsePorts.clear();
            if (dstorePorts.size() > 0) {
                //Send LIST message to all connected dstores
                for (Integer i : dstorePorts) {
                    System.out.println("[Sent to dstore " + i + "]: " + Protocol.LIST_TOKEN);
                    dstoreWriters.get(i).println(Protocol.LIST_TOKEN);
                }
                //Wait for all replies
                ExecutorService executor = Executors.newSingleThreadExecutor();
                Future<String> future = executor.submit(new ListResponseTask());
                try {
                    future.get(timeout, TimeUnit.MILLISECONDS);
                    //All list responses received
                    System.out.println("Received all LIST responses from dstores, continuing with rebalance");
                } catch(Exception e) {
                    //Timed out
                    System.out.println("Did not receive all LIST responses from dstores, continuing with rebalance");
                }
                //Get list of files as strings
                String[] fileList = getFileList().split(" "); //First element is space
                ArrayList<String> uniqueListResponseFiles = new ArrayList<>();
                listResponses.forEach((k,v) -> {
                    for (String s : v) {
                        if (!uniqueListResponseFiles.contains(s)) {
                            uniqueListResponseFiles.add(s);
                        }
                    }
                });
                //Remake index to match fileList
                HashMap<String, IndexEntry> indexOld = new HashMap<>();
                index.forEach((k, v) -> {
                    indexOld.put(k, v);
                });
                index.clear();
                for (String file : uniqueListResponseFiles) {
                    ArrayList<String> fileDstores = new ArrayList<>();
                    listResponses.forEach((k, v) -> {
                        if (v.contains(file)) {
                            fileDstores.add(String.valueOf(k));
                        }
                    });
                    index.put(file, new IndexEntry(indexOld.get(file).getFileSize(), "store complete", fileDstores));
                }
                HashMap<String, HashMap<String, ArrayList<String>>> filesToSend = new HashMap<>(); // <dstore(to send from), <filename, dstores(to send to)>>
                HashMap<String, ArrayList<String>> filesToRemove = new HashMap<>(); // <dstore, filenames>
                //For each file listed in index -> do 2 passes
                for (int i = 1; i < fileList.length; i++) {
                    String filename = fileList[i];
                    //Pass1: ensure files not listed are removed
                    if (!uniqueListResponseFiles.contains(filename) || index.get(filename).getState() == "remove in progress") {
                        index.remove(filename);
                        continue;
                    }
                    //Pass2: ensure files are stored R times
                    int count = 0;
                    int sender = 0;
                    ArrayList<Integer> dstoresWithoutFilename = new ArrayList<>();
                    if (dstorePorts.size() >= r) {
                        for (Integer port : listResponsePorts) {
                            if (listResponses.get(port).contains(filename)) {
                                count += 1;
                                if (count > r) {
                                    index.get(filename).removeDstore(String.valueOf(port));
                                    listResponses.get(port).remove(filename);
                                    //add to port's remove list
                                    addToRemoveList(filesToRemove, port, filename);
                                } else {
                                    sender = port;
                                }
                            } else {
                                dstoresWithoutFilename.add(port);
                            }
                        }
                        if (count < r) {
                            for (int e = 0; e < r-count; e++) {
                                //Choose dstore not containing filename, that has the least files stored, to copy filename to
                                Integer smallestWithout = dstoresWithoutFilename.get(0);
                                for (Integer dstoreWithout : dstoresWithoutFilename) {
                                    if (listResponses.get(dstoreWithout).size() < smallestWithout) {
                                        smallestWithout = dstoreWithout;
                                    }
                                }
                                int chosenPort = smallestWithout;
                                index.get(filename).addDstore(String.valueOf(chosenPort));
                                listResponses.get(chosenPort).add(filename);
                                //add chosenPort to send list of sender
                                addToSendList(filesToSend, chosenPort, filename, sender);
                            }
                        }
                    } else {
                        System.out.println("Rebalance operation cannot ensure files are stored R times since <R dstores are connected");
                    }
                }
                //Ensure files are stored evenly
                double toStore = (r*uniqueListResponseFiles.size()) / dstorePorts.size();
                int minToStore = (int)(Math.floor(toStore));
                int maxToStore = (int)(Math.ceil(toStore));
                AtomicBoolean changed = new AtomicBoolean(true);
                while (changed.get()) {
                    changed.set(false);
                    listResponses.forEach((k,v) -> {
                        if (v.size() > maxToStore) {
                            AtomicReference<String> actualSelected = new AtomicReference<>();
                            AtomicInteger availablePort = new AtomicInteger();
                            availablePort.set(-1);
                            //Find dstore with space to move file to
                            for (int i = 0; i < v.size(); i++) {
                                String selectedFile = v.get(i);
                                //Check if selected file can be moved to a dstore
                                listResponses.forEach((k1,v1) -> {
                                    if (!v1.contains(selectedFile) && v1.size() < maxToStore) {
                                        availablePort.set(k1);
                                        actualSelected.set(selectedFile);
                                    }
                                });
                            }
                            //Move file to other dstore
                            if (availablePort.get() != -1) {
                                v.remove(actualSelected.get());
                                index.get(actualSelected.get()).removeDstore(String.valueOf(k));
                                listResponses.get(availablePort.get()).add(actualSelected.get());
                                index.get(actualSelected.get()).addDstore(String.valueOf(availablePort.get()));
                                addToRemoveList(filesToRemove, k, actualSelected.get());
                                addToSendList(filesToSend, availablePort.get(), actualSelected.get(), k);
                                changed.set(true);
                            }
                        }
                    });
                }
                //Then create lists to send to dstores
                for (Integer dstore : dstorePorts) {
                    //Files to send
                    String toSend = "";
                    if (filesToSend.containsKey(String.valueOf(dstore))) {
                        HashMap<String, ArrayList<String>> map = filesToSend.get(String.valueOf(dstore));
                        toSend += map.size();
                        AtomicReference<String> aString = new AtomicReference<>("");
                        map.forEach((k, v) -> {
                            aString.set(aString.get() + " " + k);
                            aString.set(aString.get() + " " + v.size());
                            for (String s : v) {
                                aString.set(aString.get() + " " + s);
                            }
                        });
                        toSend += aString.get();
                    }
                    //Files to remove
                    String toRemove = "";
                    if (filesToRemove.containsKey(String.valueOf(dstore))) {
                        toRemove += filesToRemove.get(String.valueOf(dstore)).size();
                        for (String file : filesToRemove.get(String.valueOf(dstore))) {
                            toRemove += " " + file;
                        }
                    }
                    //Combine strings and send to dstore
                    if (toSend != "" || toRemove != "") {
                        if (toSend == "") {
                            toSend = "0";
                        }
                        if (toRemove == "") {
                            toRemove = "0";
                        }
                        expectedRebalanceAckCount.addAndGet(1);
                        String message = Protocol.REBALANCE_TOKEN + " " + toSend + " " + toRemove;
                        System.out.println("[Sent to dstore "+ dstore +"]: " + message);
                        dstoreWriters.get(dstore).println(message);
                    }
                }
                if (expectedRebalanceAckCount.get() > 0) {
                    System.out.println("Rebalancing complete, waiting for acks from dstores");
                }
            } else {
                System.out.println("no files moved; no dstores connected");
                index.clear();
            }
            //Wait for acks
            if (expectedRebalanceAckCount.get() > 0) {
                ExecutorService executor2 = Executors.newSingleThreadExecutor();
                Future<String> future2 = executor2.submit(new RebalanceAckTask());
                try {
                    future2.get(timeout, TimeUnit.MILLISECONDS);
                    //All rebalace acks received
                    System.out.println("Received all rebalance acks from dstores");
                } catch(Exception e) {
                    //Timed out
                    System.out.println("Did not receive all rebalance acks from dstores, no action taken");
                }
            } else {
                System.out.println("Rebalancing complete, no rebalance commands need to be sent");
            }
            if (repeat) {
                new Thread(new waitThenRebalanceThread()).start();
            }
            outputIndex();
            rebalancing.set(false);
            if (dequeuePort != -1) {
                Integer portObj = dequeuePort;
                joinQueue.remove(portObj);
            }
        }

        private void addToRemoveList(HashMap<String, ArrayList<String>> map, int port, String filename) {
            if (!map.containsKey(String.valueOf(port))) {
                ArrayList<String> newFileList = new ArrayList<>();
                newFileList.add(filename);
                map.put(String.valueOf(port), newFileList);
            } else {
                if (!map.get(String.valueOf(port)).contains(filename)) {
                    map.get(String.valueOf(port)).add(filename);
                }
            }
        }

        private void addToSendList(HashMap<String, HashMap<String, ArrayList<String>>> map, int chosenPort, String filename, int sender) {
            if (!map.containsKey(String.valueOf(sender))) {
                HashMap<String, ArrayList<String>> newMap = new HashMap<>();
                ArrayList<String> newList = new ArrayList<>();
                newList.add(String.valueOf(chosenPort));
                newMap.put(filename, newList);
                map.put(String.valueOf(sender), newMap);
            } else {
                if (!map.get(String.valueOf(sender)).containsKey(filename)) {
                    ArrayList<String> newList = new ArrayList<>();
                    newList.add(String.valueOf(chosenPort));
                    map.get(String.valueOf(sender)).put(filename, newList);
                } else {
                    map.get(String.valueOf(sender)).get(filename).add(String.valueOf(chosenPort));
                }
            }
        }
    }
    class ListResponseTask implements Callable<String>{
        public ListResponseTask() {
        }
        @Override
        public String call() {
            while (!listResponsePorts.containsAll(dstorePorts)) {}
            return "Received list responses from all connected dstores";
        }
    }

    class RebalanceAckTask implements Callable<String>{
        public RebalanceAckTask() {
        }
        @Override
        public String call() {
            while (rebalanceAcks.size() < expectedRebalanceAckCount.get()) {}
            return "Received all rebalance acks from dstores";
        }
    }

    //Returns list of stored files' filenames as string separated by spaces - has preceding space
    private static String getFileList() {
        ArrayList<String> fileNamesList = new ArrayList<>();
        String fileNames = "";
        index.forEach((k,v) -> {
            if (v.getState().equals("store complete")) {
                fileNamesList.add(k);
            }
        });
        for (String filename : fileNamesList) {
            fileNames += " " + filename;
        }
        return fileNames;
    }

    //Returns list of n ordered(least files stored) Dstore ports as a space-seperated string - has preceding space
    private static String getNOrderedDstorePorts(int n) {
        String list = "";
        ConcurrentHashMap<Integer, ArrayList<Integer>> indexes = new ConcurrentHashMap<>(); // <count, indexes>
        for (int i = 0; i < dstorePorts.size(); i++) {
            AtomicInteger count = new AtomicInteger();
            count.set(0);
            int in = i;
            index.forEach((k,v) -> {
                if (v.getDstores().contains(String.valueOf(dstorePorts.get(in)))) {
                    count.addAndGet(1);
                }
            });
            if (!indexes.containsKey(count.get())) {
                ArrayList<Integer> newList = new ArrayList<>();
                newList.add(dstorePorts.get(i));
                indexes.put(count.get(), newList);
            } else {
                indexes.get(count.get()).add(dstorePorts.get(i));
            }
        }
        TreeMap<Integer, ArrayList<Integer>> sorted = new TreeMap<>(indexes);
        ArrayList<Integer> sortedPorts = new ArrayList<>();
        for (Map.Entry<Integer, ArrayList<Integer>> entry : sorted.entrySet()) {
            for (Integer port : entry.getValue()) {
                sortedPorts.add(port);
            }
        }
        int counter = n;
        for (Integer port : sortedPorts) {
            if (counter > 0) {
                counter -= 1;
                list += " " + String.valueOf(port);
            }
        }
        return list;
    }

    //Waits the length of the rebalance period, then invokes a rebalance operation
    public class waitThenRebalanceThread implements  Runnable {
        public void run() {
            try {
                Thread.sleep(rebalancePeriod * 1000);
                new Thread(new rebalanceThread(true, -1)).start();
            } catch (Exception e) {
                System.out.println("ERROR: Rebalance wait thread ran into an issue: " + e);
            }
        }
    }

    //Prints the current state of index
    private static void outputIndex() {
        AtomicReference<String> s = new AtomicReference<>("");
        index.forEach((k,v) -> {
            s.set(s.get() + " " + k + ":");
            s.set(s.get() + " " + Arrays.toString(v.getDstores().toArray()));
        });
        System.out.println("#Current index: " + s.get());
    }
}