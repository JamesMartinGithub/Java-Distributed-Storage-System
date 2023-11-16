import java.util.ArrayList;

public class IndexEntry {
    private int fileSize;
    private String state;
    private ArrayList<String> dstores;

    public IndexEntry(int fileSize, String state, ArrayList<String> dstores) {
        this.fileSize = fileSize;
        this.state = state;
        this.dstores = dstores;
    }

    public int getFileSize() {
        return fileSize;
    }

    public void setFileSize(int fileSize) {
        this.fileSize = fileSize;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public ArrayList<String> getDstores() {
        return dstores;
    }

    public void setDstores(ArrayList<String> dstores) {
        this.dstores = dstores;
    }

    public void addDstore(String dstore) {
        if (!this.dstores.contains(dstore)) {
            this.dstores.add(dstore);
        }
    }

    public void removeDstore(String dstore) {
        this.dstores.remove(dstore);
    }

    public boolean dstoreContains(String dstore) {
        if (this.dstores.contains(dstore)) {
            return true;
        } else {
            return false;
        }
    }
}