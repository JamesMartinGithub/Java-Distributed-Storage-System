import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Dstore {
    static private int port; //Port to listen on
    static private int cport; //Controller's port number
    static private int timeout; //Timeout length
    static private String file_folder; //Folder in which to store data
    static private PrintWriter cOut;
    static private File directory;
    static private ConcurrentHashMap<String, Integer> fileList;
    static private AtomicInteger rebalanceAckCount = new AtomicInteger();
    static private AtomicInteger rebalanceStoreCount = new AtomicInteger();

    public static void main(String[] args) {
        port = Integer.parseInt(args[0]);
        cport = Integer.parseInt(args[1]);
        timeout = Integer.parseInt(args[2]);
        file_folder = args[3];
        fileList = new ConcurrentHashMap<>();
        rebalanceAckCount.set(0);
        rebalanceStoreCount.set(0);
        clearFolder();
        new Thread() {
            public void run() {
                openSocketForClientsAndDstores();
            }
        }.start();
        connectToController();
    }

    public static void clearFolder() {
        directory = new File(file_folder);
        File[] contents = directory.listFiles();
        for (File file : contents) {
            file.delete();
        }
    }

    public static void connectToController() {
        Socket controllerSocket = null;
        try {
            controllerSocket = new Socket(InetAddress.getLoopbackAddress(), cport);
            cOut = new PrintWriter(controllerSocket.getOutputStream(), true);
            BufferedReader cIn = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));
            cOut.println(Protocol.JOIN_TOKEN + " " + port);
            System.out.println("[Sent to controller]: " + Protocol.JOIN_TOKEN + " " + port);
            String line;
            while((line = cIn.readLine()) != null) {
                System.out.println("{Received from controller}: " + line);
                String[] messageParts = line.split(" ");
                switch (messageParts[0]) {
                    case Protocol.REMOVE_TOKEN: {
                        String filename = messageParts[1];
                        File file = new File(file_folder + File.separator + filename);
                        if (file.exists()) {
                            file.delete();
                        }
                        if (fileList.containsKey(filename)) {
                            fileList.remove(filename);
                            System.out.println("removed file: " + filename + " in directory");
                            System.out.println("[Sent to controller]: " + Protocol.REMOVE_ACK_TOKEN + " " + filename);
                            cOut.println(Protocol.REMOVE_ACK_TOKEN + " " + filename);
                        } else {
                            System.out.println("[Sent to controller]: " + Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + filename);
                            cOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + filename);
                        }
                        break; }
                    case Protocol.LIST_TOKEN:
                        String files = getFileList();
                        System.out.println("[Sent to controller]: " + Protocol.LIST_TOKEN + files);
                        cOut.println(Protocol.LIST_TOKEN + files);
                        break;
                    case Protocol.REBALANCE_TOKEN:
                        rebalanceAckCount.set(0);
                        rebalanceStoreCount.set(0);
                        int filesToSendNum = Integer.valueOf(messageParts[1]);
                        //Parse message
                        int n = 2;
                        AtomicInteger targetAcks = new AtomicInteger(0);
                        //Parse files to send
                        HashMap<String, ArrayList<Integer>> filesAndPorts = new HashMap<>();
                        for (int i = 0; i < filesToSendNum; i++) {
                            String filename = messageParts[n];
                            n += 1;
                            int portsNum = Integer.parseInt(messageParts[n]);
                            n += 1;
                            ArrayList<Integer> portsToSendTo = new ArrayList<>();
                            for (int e = 0; e < portsNum; e++) {
                                portsToSendTo.add(Integer.parseInt(messageParts[n]));
                                n += 1;
                            }
                            targetAcks.addAndGet(portsNum);
                            filesAndPorts.put(filename, portsToSendTo);
                        }
                        //Parse files to remove
                        int filesToRemoveNum = Integer.valueOf(messageParts[n]);
                        n += 1;
                        ArrayList<String> filesToDeleteAfterRebalance = new ArrayList<>();
                        for (int i = 0; i < filesToRemoveNum; i++) {
                            String filename = messageParts[n];
                            //Remove filename (if not needed in sending operation)
                            if (filesAndPorts.containsKey(filename)) {
                                //Remove after sending
                                filesToDeleteAfterRebalance.add(filename);
                            } else {
                                //Remove now
                                fileList.remove(filename);
                                File file = new File(file_folder + File.separator + filename);
                                if (file.exists()) {
                                    file.delete();
                                }
                                System.out.println("Removed file: " + filename);
                            }
                            n += 1;
                        }
                        //Send files to ports
                        filesAndPorts.forEach((k, v) -> {
                            new Thread(new Dstore() .new SendFilesThread(k, v, targetAcks.get(), filesToDeleteAfterRebalance)).start();
                        });
                        break;
                    default:
                        System.out.println("ERROR: received unrecognized controller command: " + messageParts[0]);
                        break;
                }
            }
        } catch(Exception e) { System.err.println("ERROR: " + e);
        } finally {
            if (controllerSocket != null)
                try { controllerSocket.close(); } catch (IOException e) { System.err.println("ERROR: " + e); }
        }
    }

    public static void openSocketForClientsAndDstores() {
        ServerSocket dstoreSocket = null;
        try {
            dstoreSocket = new ServerSocket(port);
            while (true) {
                try {
                    Socket clientOrDstore = dstoreSocket.accept();
                    System.out.println("Client or Dstore connected");
                    new Thread(new Dstore().new ClientOrDstoreThread(clientOrDstore)).start();
                } catch(Exception e) { System.err.println("ERROR1: " + e); }
            }
        } catch(Exception e) { System.err.println("ERROR2: " + e);
        } finally {
            if (dstoreSocket != null)
                try { dstoreSocket.close(); } catch (IOException e) { System.err.println("ERROR: " + e); }
        }
    }

    class ClientOrDstoreThread implements Runnable {
        Socket clientOrDstore;
        ClientOrDstoreThread(Socket socket) {
            clientOrDstore=socket;
        }
        public void run() {
            try {
                InputStream inStream = clientOrDstore.getInputStream();
                BufferedReader in = new BufferedReader(new InputStreamReader(inStream));
                OutputStream outStream = clientOrDstore.getOutputStream();
                PrintWriter out = new PrintWriter(outStream, true);
                String line;
                while((line = in.readLine()) != null) {
                    System.out.println("{Received from client or dstore}: " + line);
                    String[] messageParts = line.split(" ");
                    switch (messageParts[0]) {
                        case Protocol.STORE_TOKEN: {
                            System.out.println("[Sent to client]: " + Protocol.ACK_TOKEN);
                            out.println(Protocol.ACK_TOKEN);
                            String filename = messageParts[1];
                            int filesize = Integer.parseInt(messageParts[2]);
                            byte[] fileData = inStream.readNBytes(filesize);
                            System.out.println("{Received from client}: " + filename + " data");
                            File file = new File(file_folder + File.separator + filename);
                            file.createNewFile();
                            FileOutputStream fileOutStream = new FileOutputStream(file, false);
                            fileOutStream.write(fileData);
                            fileOutStream.close();
                            System.out.println("Created/updated file: " + filename + " in directory");
                            if (fileList.containsKey(filename)) {
                                fileList.remove(filename);
                            }
                            fileList.put(filename, filesize);
                            System.out.println("[Sent to client]: " + Protocol.STORE_ACK_TOKEN + " " + filename);
                            cOut.println(Protocol.STORE_ACK_TOKEN + " " + filename);
                            break; }
                        case Protocol.LOAD_DATA_TOKEN: {
                            String filename = messageParts[1];
                            File file = new File(file_folder + File.separator + filename);
                            if (fileList.containsKey(filename) && file.exists()) {
                                FileInputStream fileInStream = new FileInputStream(file);
                                byte[] fileData = fileInStream.readNBytes(fileList.get(filename));
                                fileInStream.close();
                                System.out.println("[Sent to client]: '" + filename + " data'");
                                outStream.write(fileData);
                            } else {
                                System.out.println("ERROR: couldn't find file " + filename);
                                clientOrDstore.close();
                            }
                            break; }
                        case Protocol.REBALANCE_STORE_TOKEN:
                            System.out.println("[Sent to dstore]: " + Protocol.ACK_TOKEN);
                            out.println(Protocol.ACK_TOKEN);
                            String filename = messageParts[1];
                            int filesize = Integer.parseInt(messageParts[2]);
                            byte[] fileData = inStream.readNBytes(filesize);
                            System.out.println("{Received from dstore}: " + filename + " data");
                            File file = new File(file_folder + File.separator + filename);
                            file.createNewFile();
                            FileOutputStream fileOutStream = new FileOutputStream(file, false);
                            fileOutStream.write(fileData);
                            fileOutStream.close();
                            System.out.println("Created/updated file: " + filename + " in directory");
                            if (fileList.containsKey(filename)) {
                                fileList.remove(filename);
                            }
                            fileList.put(filename, filesize);
                            break;
                        default:
                            System.out.println("ERROR: received unrecognized client or dstore command: " + messageParts[0]);
                            break;
                    }
                }
            } catch(Exception e) {
                System.err.println("ERROR (likely client or dstore crash): " + e);
            } finally {
                try {clientOrDstore.close();} catch(Exception e) {}
                System.out.println("Client or Dstore disconnected");
            }
        }
    }

    class SendFilesThread implements Runnable {
        String filename;
        ArrayList<Integer> portList;
        int targetAcks;
        ArrayList<String> fileDeleteList;
        public SendFilesThread(String file, ArrayList<Integer> ports, int acks, ArrayList<String> deleteList) {
            filename = file;
            portList = ports;
            targetAcks = acks;
            fileDeleteList = deleteList;
        }
        public void run() {
            //Send file to ports
            for (Integer i : portList) {
                new Thread(new SendFilesHelperThread(filename, i, targetAcks, fileDeleteList)).start();
            }
        }
    }
    class SendFilesHelperThread implements Runnable {
        String filename;
        int port;
        int targetAcks;
        ArrayList<String> fileDeleteList;
        public SendFilesHelperThread(String file, int portNum, int acks, ArrayList<String> deleteList) {
            filename = file;
            port = portNum;
            targetAcks = acks;
            fileDeleteList = deleteList;
        }
        public void run() {
            //Open socket
            Socket otherDstore = null;
            try {
                otherDstore = new Socket(InetAddress.getLoopbackAddress(), port);
                OutputStream outStream = otherDstore.getOutputStream();
                PrintWriter out = new PrintWriter(outStream, true);
                BufferedReader in = new BufferedReader(new InputStreamReader(otherDstore.getInputStream()));
                if (fileList.containsKey(filename)) {
                    String message = Protocol.REBALANCE_STORE_TOKEN + " " + filename + " " + fileList.get(filename);
                    System.out.println("[Sent to dstore " + port + "]: " + message);
                    out.println(message);
                    //Wait for ACK or timeout
                    ExecutorService executor = Executors.newSingleThreadExecutor();
                    Future<String> future = executor.submit(new RebalanceStoreAckTask(in));
                    try {
                        String response = future.get(timeout, TimeUnit.MILLISECONDS);
                        if (!response.equals("ERROR")) {
                            //Received ACK
                            //Get file content
                            File file = new File(file_folder + File.separator + filename);
                            if (file.exists()) {
                                FileInputStream fileStream = new FileInputStream(file);
                                //Send file content
                                outStream.write(fileStream.readNBytes(fileList.get(filename)));
                                fileStream.close();
                                rebalanceAckCount.addAndGet(1);
                                if (rebalanceAckCount.get() >= targetAcks) {
                                    System.out.println("[Sent to controller]: " + Protocol.REBALANCE_COMPLETE_TOKEN);
                                    cOut.println(Protocol.REBALANCE_COMPLETE_TOKEN);
                                }
                            } else {
                                System.out.println("ERROR: file " + filename + " does not exist");
                            }
                        } else {
                            System.out.println("ERROR: ");
                        }
                    } catch(Exception e) {
                        //Received no ACK
                        System.out.println("Rebalance store timed out, no ack received");
                    }
                    //Remove file after send if needed
                    rebalanceStoreCount.addAndGet(1);
                    if (rebalanceStoreCount.get() >= targetAcks) {
                        //Last store operation, rest of files can be deleted
                        for (String fileS : fileDeleteList) {
                            fileList.remove(fileS);
                            System.out.println("Removed file: " + fileS);
                            File file = new File(file_folder + File.separator + fileS);
                            if (file.exists()) {
                                file.delete();
                            }
                        }
                    }
                } else {
                    System.out.println("ERROR: Cannot find " + filename + " to send to dstore " + port);
                }
            } catch(Exception e) { System.err.println("ERROR: " + e);
            } finally {
                if (otherDstore != null)
                    try { otherDstore.close(); } catch (IOException e) { System.err.println("ERROR: " + e); }
            }
            }
    }
    class RebalanceStoreAckTask implements Callable<String> {
        BufferedReader in;
        public RebalanceStoreAckTask(BufferedReader inReader) {
            in = inReader;
        }
        @Override
        public String call() {
            try {
                String line;
                while ((line = in.readLine()) != null) {
                    if (line.equals(Protocol.ACK_TOKEN)) {
                        System.out.println("{Received from dstore}: " + Protocol.ACK_TOKEN);
                        return "Received ACK";
                    } else {
                        return "ERROR: Unrecognized dstore response: " + line;
                    }
                }
                return "ERROR";
            } catch(Exception e) {
                System.out.println("ERROR: " + e);
                return "ERROR";
            }
        }
    }

    //Returns list of stored files' filenames as string separated by spaces - has preceding space
    private static String getFileList() {
        ArrayList<String> fileNamesList = new ArrayList<>();
        String fileNames = "";
        fileList.forEach((k,v) -> {
            fileNamesList.add(k);
        });
        for (String filename : fileNamesList) {
            fileNames += " " + filename;
        }
        return fileNames;
    }
}