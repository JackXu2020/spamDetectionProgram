package spamdetection.rpcserver;

import lombok.Data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

@Data
public class WordInfo {

    private String word;
    private int hamNum;
    private int spamNum;
    private float wordHamPossibility;
    private float wordSpamPossibility;

    public static WordInfo getInstanceByByteArray(byte[] data) throws Exception {

        WordInfo wordInfo = new WordInfo();
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(in);
        wordInfo.setWord(dis.readUTF());
        wordInfo.setHamNum(dis.readInt());
        wordInfo.setSpamNum(dis.readInt());
        wordInfo.setWordHamPossibility(dis.readFloat());
        wordInfo.setWordSpamPossibility(dis.readFloat());
        dis.close();
        in.close();
        return wordInfo;
    }

    public byte[] saveInstanceToByteArray() throws Exception{
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        dos.writeUTF(word);
        dos.writeInt(hamNum);
        dos.writeInt(spamNum);
        dos.writeFloat(wordHamPossibility);
        dos.writeFloat(wordSpamPossibility);
        dos.close();
        out.close();
        return out.toByteArray();
    }
























}
