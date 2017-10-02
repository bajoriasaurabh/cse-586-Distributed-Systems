package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.lang.Integer.parseInt;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 */
public class GroupMessengerActivity extends Activity {
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final ArrayList<String> REMOTE_PORT = new ArrayList<String>(Arrays.asList("11108", "11112", "11116", "11120", "11124"));
    static final int SERVER_PORT = 10000;
    public static String messageType[] = {"initial", "proposed", "agreed"};
    public static List<Message> queue = new ArrayList<Message>();
    static String myPort;
    static double initialSeqNum = 0;
    static int messageID;


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((parseInt(portStr) * 2));

        if (myPort.contentEquals(REMOTE_PORT.get(0)))
            initialSeqNum = Double.parseDouble("0." + REMOTE_PORT.get(0));
        else if (myPort.contentEquals(REMOTE_PORT.get(1)))
            initialSeqNum = Double.parseDouble("0." + REMOTE_PORT.get(1));
        else if (myPort.contentEquals(REMOTE_PORT.get(2)))
            initialSeqNum = Double.parseDouble("0." + REMOTE_PORT.get(2));
        else if (myPort.contentEquals(REMOTE_PORT.get(3)))
            initialSeqNum = Double.parseDouble("0." + REMOTE_PORT.get(3));
        else if (myPort.contentEquals(REMOTE_PORT.get(4)))
            initialSeqNum = Double.parseDouble("0." + REMOTE_PORT.get(4));

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }

        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        final EditText editText = (EditText) findViewById(R.id.editText1);
        findViewById(R.id.button4).setOnClickListener(
                new View.OnClickListener() {
                    @Override
                    public void onClick(View v) {
                        //taken from PA1
                        String msg = editText.getText().toString() + "\n";
                        editText.setText(""); // This is one way to reset the input box.
                        TextView localTextView = (TextView) findViewById(R.id.textView1);
                        localTextView.append("\t" + msg); // This is one way to display a string.
                        TextView remoteTextView = (TextView) findViewById(R.id.textView1);
                        remoteTextView.append("\n");
                        messageID = messageID + 1;
                        Message newMessage = new Message(msg, myPort, messageID, messageType[0]);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, newMessage);
                    }
                });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    private void portFailure(String remotePort) {
        REMOTE_PORT.remove(REMOTE_PORT.indexOf(remotePort));
        REMOTE_PORT.trimToSize();
    }

    private class ServerTask extends AsyncTask<ServerSocket, Message, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Uri newUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");
            try {
                do {
                    Socket socketReceived = serverSocket.accept();
                    ObjectInputStream objectInputStream = new ObjectInputStream(socketReceived.getInputStream());
                    Message message = (Message) objectInputStream.readObject();
                    if (message.msgType.equals(messageType[0])) {
                        message.proposedSequenceNum = ++initialSeqNum;
                        message.deliveryType = "undeliverable";
                        message.msgType = messageType[1];
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(socketReceived.getOutputStream());
                        objectOutputStream.writeObject(message);
                    }
                    if (message.msgType.equals(messageType[2])) {
                        ContentValues newData = new ContentValues();
                        newData.put("key", Integer.toString((int) message.proposedSequenceNum - 1));
                        newData.put("value", message.text);
                        getContentResolver().insert(newUri, newData);
                        publishProgress(message);
                    }
                } while (true);

            } catch (IOException e) {
                e.printStackTrace();
                Log.e(TAG, "File write failed");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            return null;
        }

        //taken from PA1
        protected void onProgressUpdate(Message... messages) {
            String strReceived = messages[0].text.trim();
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\n");
            TextView localTextView = (TextView) findViewById(R.id.textView1);
            localTextView.append("\n");
            String filename = "SimpleMessengerOutput";
            String string = strReceived + "\n";
            FileOutputStream outputStream;
            try {
                outputStream = openFileOutput(filename, Context.MODE_PRIVATE);
                outputStream.write(string.getBytes());
                outputStream.close();
            } catch (Exception e) {
                Log.e(TAG, "File write failed");
            }
        }

        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }


    }

    private class ClientTask extends AsyncTask<Message, Void, Void> {

        @Override
        protected Void doInBackground(Message... msgs) {
            if (msgs[0].msgType.equals(messageType[0])) {
                for (int i = 0; i <= REMOTE_PORT.size(); i++) {
                    try {
                        msgs[0].receiverPort = REMOTE_PORT.get(i);
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), parseInt(REMOTE_PORT.get(i)));
                        socket.setSoTimeout(500);
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                        objectOutputStream.writeObject(msgs[0]);
                        ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
                        Message message = (Message) objectInputStream.readObject();
                        if (message.msgType.equals(messageType[1]) && message.deliveryType.equals("undeliverable")) {
                            queue.add(message);
                        }
                        objectOutputStream.close();
                        socket.close();
                    } catch (SocketTimeoutException se) {
                        Log.v(TAG, "Failed port" + REMOTE_PORT.get(i));
                        portFailure(REMOTE_PORT.get(i));
                    } catch (UnknownHostException e) {
                        Log.e(TAG, "ClientTask UnknownHostException");
                    } catch (IOException e) {
                        e.printStackTrace();
                        Log.e(TAG, "ClientTask socket IOException");
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }
            if (queue.size() == REMOTE_PORT.size()) {
                Collections.reverse(queue);
                queue.get(0).msgType = messageType[2];
                queue.get(0).deliveryType = "deliverable";
                for (int i = 0; i <= REMOTE_PORT.size(); i++) {
                    try {
                        if (queue.get(0).msgType.equals(messageType[2]) && queue.get(0).deliveryType.equals("deliverable")) {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT.get(i)));
                            socket.setSoTimeout(500);
                            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                            objectOutputStream.writeObject(queue.get(0));
                            objectOutputStream.close();
                            socket.close();
                        }
                    } catch (SocketTimeoutException se) {
                        Log.v(TAG, "Failed port" + REMOTE_PORT.get(i));
                        portFailure(REMOTE_PORT.get(i));
                    } catch (IOException e) {
                        e.printStackTrace();
                        Log.e(TAG, "ClientTask socket IOException");
                    }
                }
                queue.clear();
            }
            return null;
        }

    }
}

class Message implements Serializable {
    String text;
    String senderPort;
    String msgType;
    double proposedSequenceNum;
    String deliveryType;
    String receiverPort;
    int messageID;

    Message(String text, String portNumber, int count, String msgType) {
        this.text = text;
        this.senderPort = portNumber;
        this.msgType = msgType;
        messageID = count;
    }
}
