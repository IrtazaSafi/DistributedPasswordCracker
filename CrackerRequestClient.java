import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Scanner;

/**
 * Created by irtaza on 4/28/2015.
 */
public class CrackerRequestClient {

    public static String magic;
    public static String ID = null;
    public static String iPAddress;
    public static String portNumber;
    public static String serverIP;
    public static String serverPort;
    public static DatagramSocket outSock;
    public static DatagramSocket inSock;
    public static volatile String currJobID;

    public static String recieveMessage() throws IOException {
        byte[] receiveData = new byte[1024];
        byte[] sendData = new byte[1024];
        DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
        inSock.receive(receivePacket);
        String sentence = new String(receivePacket.getData(),0,receivePacket.getLength());
        return sentence;
    }
    public static synchronized void SendMessage(String PortNumber,String IP,String message) throws IOException {
        InetAddress IPAddress = InetAddress.getByName(IP);
        byte[] sendData = new byte[1024];
        byte[] receiveData = new byte[1024];
        sendData = message.getBytes();
        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, Integer.parseInt(PortNumber));
        outSock.send(sendPacket);
        // clientSocket.close();

    }
    public static String HashPassword(String plaintext) throws NoSuchAlgorithmException {
        MessageDigest m = MessageDigest.getInstance("MD5");
        m.reset();
        m.update(plaintext.getBytes());
        byte[] digest = m.digest();
        BigInteger bigInt = new BigInteger(1, digest);
        String hashtext = bigInt.toString(16);
// Now we need to zero pad it if you actually want the full 32 chars.
        while (hashtext.length() < 32) {
            hashtext = "0" + hashtext;
        }
        return hashtext;
    }

    public static String HashPasswordTwo(String plaintext) throws NoSuchAlgorithmException {
        MessageDigest m = MessageDigest.getInstance("MD5");
        m.reset();
        m.update(plaintext.getBytes());
        byte[] digest = m.digest();
        BigInteger bigInt = new BigInteger(1, digest);
        String hashtext = bigInt.toString(16);
// Now we need to zero pad it if you actually want the full 32 chars.

        return hashtext;
    }
    public static String HashPasswordThree(String input) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        //String input = "your string";
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.update(input.getBytes("UTF-8"));
        byte[] hash = digest.digest();
        String hashtext = hash.toString();
        return hashtext;
    }
    public static void messageHandler() throws IOException {
        while (true) {
            String message = recieveMessage();

            String [] mV = message.split(",");
            if(mV[4].equals("Ping")==false){
                System.out.println(message);
            }
            //System.out.println(mV.length);
            if(mV[4].equals("800")){
              //  System.out.println("mV[4] is 800");
                currJobID = mV[5];
                ID = mV[3];
                System.out.println("currJOBID set to  " + currJobID);
            }
            if(mV[4].equals("8001")){
                //  System.out.println("mV[4] is 800");
                currJobID = mV[5];
                //ID = mV[3];
                System.out.println("currJOBID set to  " + currJobID);
            }


            if(mV[4].equals("Ping")){
                String reply = portNumber+","+iPAddress+","+magic+","+ID+",PingOk,NULL";
                SendMessage(mV[0],mV[1],reply);
            }

            if(mV[4].equals("000")){
                System.out.println("PASSWORD FOUND : " + mV[6]);
            }

        }

    }



    public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
        magic = "15440";
        serverPort = "3530";
        serverIP = "192.168.254.1";
        portNumber = args[0];
        inSock = new DatagramSocket(Integer.parseInt(portNumber));
        iPAddress = InetAddress.getLocalHost().getHostAddress();
        outSock = new DatagramSocket();
        System.out.println("Request Client Active on Port:" + " " + portNumber + " " + "and IP Address " + iPAddress);

        // Make New Thread to Listen to Incoming Messages on inSock
        Thread messageListener = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    messageHandler();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }); messageListener.start();

        // Make Loop For getting User Input

        while(true) {
            System.out.println("Enter Password For Cracking");
            Scanner s = new Scanner(System.in);
            String userInput = s.nextLine();
            final  String [] in = userInput.split(",");

            if(in[0].equals("Crack")) {
                if (ID == null) {
                    String Message = portNumber + "," + iPAddress + "," + magic + "," + "0,8,NULL,NULL," + HashPasswordTwo(in[1]);
                    SendMessage(serverPort, serverIP, Message);
                } else {
                    String Message = portNumber + "," + iPAddress + "," + magic + "," + ID+",8,NULL,NULL," + HashPasswordTwo(in[1]);
                    SendMessage(serverPort, serverIP, Message);
                }
            }
            if(in[0].equals("CANCEL")){
                String Message = portNumber + "," + iPAddress + "," + magic + "," + ID+","+"CANCEL-JOB,"+currJobID;
                SendMessage(serverPort, serverIP, Message);
            }
        }






    }
}
