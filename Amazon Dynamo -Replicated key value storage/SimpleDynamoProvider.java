package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.List;

public class SimpleDynamoProvider extends ContentProvider {
    static final int SERVER_PORT = 10000;
    static final String TAG = SimpleDynamoActivity.class.getSimpleName();
    boolean flag;
    ArrayList<String> hashedList = new ArrayList<String>(Arrays.asList("177ccecaec32c54b82d5aaafc18a2dadb753e3b1", "208f7f72b198dadd244e61801abe1ec3a4857bc9", "33d6357cfaaf0f72991b0ecd8c56da066613c089", "abf0fd8db03e5ecb199a9b82929e9db79b909643", "c25ddd596aa7c81fa12378fa725f706d54325d12"));
    ArrayList<String> list = new ArrayList<String>(Arrays.asList("5562", "5556", "5554", "5558", "5560"));
    SQLiteDatabase sqLiteDatabase;
    String[] columnsinsert = {"KEY", "VALUE", "TIMESTAMP"};
    String[] columnsquery = {"KEY", "VALUE"};
    String portStr = null;
    boolean waitingFlag = false;
    int counter;


    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        counter = 0;
        File databasePath = getContext().getDatabasePath("SimpleDynamo");
        if (databasePath.exists()) {
            waitingFlag = true;
        }
        SQLDatabase sqlDatabase = new SQLDatabase(getContext());
        sqLiteDatabase = sqlDatabase.getWritableDatabase();
        try {
            TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
            portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            Message message = new Message();
            message.myPort = portStr;
            message.hashedMyPort = genHash(portStr);
            message.type = "JOIN";
            message.firstSuccessor = getSuccessor(portStr, 1);
            message.secondSuccessor = getSuccessor(portStr, 2);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        System.out.println("At Delete: "+selection);
        // TODO Auto-generated method stub
        Socket socket = null;
        if (selection.equals("\"*\"") || selection.equals("*")) {
            Message message = new Message();
            message.key = null;
            message.type = "*DELETE";
            for (String s : list) {
                try {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(s) * 2);
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                    objectOutputStream.writeObject(message);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.println("At delete Exception");
                }
            }
        } else if (selection.equals("\"@\"") || selection.equals("@")) {
            SQLDatabase sqlDatabase = new SQLDatabase(getContext());
            SQLiteDatabase sqLiteDatabase = sqlDatabase.getWritableDatabase();
            sqLiteDatabase.delete(SQLDatabase.TABLE_NAME, null, null);
        } else {
            for (int i = 0; i < hashedList.size() - 1; i++) {
                try {
                    if (genHash(selection).compareTo(hashedList.get(i)) > 0 && genHash(selection).compareTo(hashedList.get(i + 1)) <= 0) {
                        for (int j = i + 1; j <= i + 3; j++) {
                            Message message = new Message();
                            message.type = "DELETE";
                            message.key = selection;
                            message.keyHash = genHash(selection);
                            if (j >= hashedList.size()) {
                                message.hashedSenderPort = hashedList.get(j - hashedList.size());
                            } else {
                                message.hashedSenderPort = hashedList.get(j);
                            }
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
                        }
                        break;
                    } else if (genHash(selection).compareTo(hashedList.get(hashedList.size() - 1)) > 0 || genHash(selection).compareTo(hashedList.get(0)) == 0 ||
                            genHash(selection).compareTo(hashedList.get(0)) < 0) {
                        for (int j = 0; j <= 2; j++) {
                            Message message = new Message();
                            message.hashedSenderPort = hashedList.get(j);
                            message.type = "DELETE";
                            message.key = selection;
                            message.keyHash = genHash(selection);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
                        }
                        break;
                    }
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }
        }

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        System.out.println("At Insert: "+ values.getAsString("key"));
        try {
            String key = values.getAsString("key");
            String value = values.getAsString("value");
            String keyHash = genHash(key);
            for (int i = 0; i < hashedList.size() - 1; i++) {
                if (keyHash.compareTo(hashedList.get(i)) > 0 && keyHash.compareTo(hashedList.get(i + 1)) <= 0) {
                    for (int j = i + 1; j <= i + 3; j++) {
                        Message message = new Message();
                        message.keyHash = keyHash;
                        message.key = key;
                        message.value = value;
                        message.type = "INSERT";
                        if (j >= hashedList.size()) {
                            message.hashedSenderPort = hashedList.get(j - hashedList.size());
                            if (unHashedPort(message.hashedSenderPort).equals("5562") && flag) {
                                Thread.sleep(2000);
                                flag = false;
                            }
                        } else {
                            message.hashedSenderPort = hashedList.get(j);
                            if (unHashedPort(message.hashedSenderPort).equals("5562") && flag) {
                                Thread.sleep(2000);
                                flag = false;
                            }
                        }
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
                    }
                    break;
                } else if (keyHash.compareTo(hashedList.get(hashedList.size() - 1)) > 0 || keyHash.compareTo(hashedList.get(0)) == 0 ||
                        keyHash.compareTo(hashedList.get(0)) < 0) {

                    for (int j = 0; j <= 2; j++) {
                        Message message = new Message();
                        message.keyHash = keyHash;
                        message.key = key;
                        message.value = value;
                        message.type = "INSERT";
                        message.hashedSenderPort = hashedList.get(j);
                        if (unHashedPort(message.hashedSenderPort).equals("5562") && flag) { //
                            Thread.sleep(2000);
                            flag = false;
                        }
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
                    }

                    break;
                }
            }

            Log.v("insert", values.toString());
        } catch (Exception e) {
            Log.e("Insert", "File write failed");
        }
        return uri;
    }


    public String getSuccessor(String port, int i) {
        if (list.indexOf(port) + i < list.size())
            return list.get(list.indexOf(port) + i);
        else
            return list.get(list.indexOf(port) + i - list.size());
    }

    public String getPredecessor(String port, int i) {
        if (list.indexOf(port) - i >= 0)
            return list.get(list.indexOf(port) - i);
        else
            return list.get(list.indexOf(port) - i + list.size());
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        System.out.println("At Query: " + selection);
        MatrixCursor cursor = new MatrixCursor(columnsquery);
        if (selection.equals("\"*\"") || selection.equals("*")) {
            Message message = new Message();
            Socket socket;
            message.list = list;
            message.hashedList = hashedList;
            message.key = null;
            message.type = "*QUERY";
            List<String[]> newList = new ArrayList<String[]>();
            for (String s : message.list) {
                message.senderPort = s;
                try {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(message.senderPort) * 2);
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                    objectOutputStream.writeObject(message);
                    ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
                    List<String[]> list1;
                    list1 = (ArrayList<String[]>) objectInputStream.readObject();
                    newList.addAll(list1);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            for (String[] s : newList) {
                cursor.addRow(new Object[]{s[0], s[1]});
            }
            return cursor;
        } else if (selection.equals("\"@\"") || selection.equals("@")) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            SQLDatabase sqlDatabase = new SQLDatabase(getContext());
            SQLiteDatabase sqLiteDatabase = sqlDatabase.getReadableDatabase();
            return sqLiteDatabase.query(false, SQLDatabase.TABLE_NAME, columnsquery, null, null, null, null, null, null);
        } else {
            try {
                ArrayList<String[]> keyValuesCombined = new ArrayList<String[]>();
                for (int i = 0; i < hashedList.size() - 1; i++) {
                    if (genHash(selection).compareTo(hashedList.get(i)) > 0 && genHash(selection).compareTo(hashedList.get(i + 1)) <= 0) {
                        for (int j = i + 1; j <= i + 3; j++) {
                            ArrayList<String[]> keyValues = new ArrayList<String[]>();
                            Message message = new Message();
                            message.type = "QUERY";
                            message.key = selection;
                            message.keyHash = genHash(selection);
                            if (j >= hashedList.size()) {
                                message.hashedSenderPort = hashedList.get(j - hashedList.size());
                            } else {
                                message.hashedSenderPort = hashedList.get(j);
                            }
                            try {
                                keyValues = (ArrayList<String[]>) new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message).get();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            if (keyValues != null && keyValues.size() != 0) {
                                keyValuesCombined.addAll(keyValues);
                            }

                        }
                        int index = 0;
                        long timestamp = Long.parseLong(keyValuesCombined.get(0)[2]);
                        for (int k = 1; k < keyValuesCombined.size(); k++) {
                            if (timestamp < Long.parseLong(keyValuesCombined.get(k)[2])) {
                                timestamp = Long.parseLong(keyValuesCombined.get(k)[2]);
                                index = k;
                            }
                        }
                        cursor.addRow(new Object[]{keyValuesCombined.get(index)[0], keyValuesCombined.get(index)[1]});
                        return cursor;

                    } else if (genHash(selection).compareTo(hashedList.get(hashedList.size() - 1)) > 0 || genHash(selection).compareTo(hashedList.get(0)) == 0 ||
                            genHash(selection).compareTo(hashedList.get(0)) < 0) {
                        for (int j = 0; j <= 2; j++) {
                            Message message = new Message();
                            message.hashedSenderPort = hashedList.get(j);
                            message.type = "QUERY";
                            message.key = selection;
                            message.keyHash = genHash(selection);
                            ArrayList<String[]> keyValues = new ArrayList<String[]>();
                            try {
                                keyValues = (ArrayList<String[]>) new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message).get();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            if (keyValues != null && keyValues.size() != 0) {
                                keyValuesCombined.addAll(keyValues);

                            }

                        }
                        int index = 0;
                        long timestamp = Long.parseLong(keyValuesCombined.get(0)[2]);
                        for (int k = 1; k < keyValuesCombined.size(); k++) {
                            if (timestamp < Long.parseLong(keyValuesCombined.get(k)[2])) {
                                timestamp = Long.parseLong(keyValuesCombined.get(k)[2]);
                                index = k;
                            }
                        }
                        cursor.addRow(new Object[]{keyValuesCombined.get(index)[0], keyValuesCombined.get(index)[1]});
                        return cursor;
                    }
                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public String unHashedPort(String hashedPort) {
        try {
            if (genHash("5554").equals(hashedPort)) {
                return "5554";
            } else if (genHash("5556").equals(hashedPort)) {
                return "5556";
            } else if (genHash("5558").equals(hashedPort)) {
                return "5558";
            } else if (genHash("5560").equals(hashedPort)) {
                return "5560";
            } else if (genHash("5562").equals(hashedPort)) {
                return "5562";
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return "";
    }

    private class ServerTask extends AsyncTask<ServerSocket, Message, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            while (true) {
                try {
                    Socket socketReceived = serverSocket.accept();
                    ObjectInputStream objectInputStream = new ObjectInputStream(socketReceived.getInputStream());
                    Message message = (Message) objectInputStream.readObject();
                    if (message.type.equals("INSERT")) {
                        while (waitingFlag) {
                        }
                        System.out.println("At Insert server");
                        ContentValues values = new ContentValues();
                        values.put(SQLDatabase.KEY, message.key);
                        values.put(SQLDatabase.VALUE, message.value);
                        sqLiteDatabase.beginTransaction();
                        values.put(SQLDatabase.TIMESTAMP, System.currentTimeMillis());
                        sqLiteDatabase.insert(SQLDatabase.TABLE_NAME, null, values);
                        sqLiteDatabase.setTransactionSuccessful();
                        sqLiteDatabase.endTransaction();
                    } else if (message.type.equals("QUERY") || message.type.equals("*QUERY")) {
                        System.out.println("At Query server");
                        SQLDatabase sqlDatabase = new SQLDatabase(getContext());
                        SQLiteDatabase sqLiteDatabase = sqlDatabase.getReadableDatabase();
                        Cursor cursor;
                        if (message.type.equals("QUERY")) {
                            cursor = sqLiteDatabase.query(true, SQLDatabase.TABLE_NAME, columnsinsert, "KEY='" + message.key + "'", null, null, null, null, null);
                        } else {
                            cursor = sqLiteDatabase.query(true, SQLDatabase.TABLE_NAME, columnsinsert, null, null, null, null, null, null);

                        }
                        ArrayList<String[]> keyValues = new ArrayList<String[]>();
                        cursor.moveToFirst();
                        for (cursor.moveToFirst(); !cursor.isAfterLast(); cursor.moveToNext()) {
                            String temp[] = new String[3];
                            temp[0] = cursor.getString(cursor.getColumnIndex("key"));
                            temp[1] = cursor.getString(cursor.getColumnIndex("value"));
                            temp[2] = cursor.getString(cursor.getColumnIndex("timestamp"));
                            keyValues.add(temp);
                        }
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(socketReceived.getOutputStream());
                        objectOutputStream.writeObject(keyValues);
                    } else if (message.type.equals("*DELETE")) {
                        SQLDatabase sqlDatabase = new SQLDatabase(getContext());
                        SQLiteDatabase sqLiteDatabase = sqlDatabase.getWritableDatabase();
                        sqLiteDatabase.delete(SQLDatabase.TABLE_NAME, null, null);
                    } else if (message.type.equals("DELETE")) {
                        SQLDatabase sqlDatabase = new SQLDatabase(getContext());
                        SQLiteDatabase sqLiteDatabase = sqlDatabase.getWritableDatabase();
                        sqLiteDatabase.delete(SQLDatabase.TABLE_NAME, "KEY='" + message.key + "'", null);
                    } else if (message.type.equals(("JOIN"))) {
                        SQLDatabase sqlDatabase = new SQLDatabase(getContext());
                        SQLiteDatabase sqLiteDatabase = sqlDatabase.getReadableDatabase();
                        Cursor cursor;
                        cursor = sqLiteDatabase.query(true, SQLDatabase.TABLE_NAME, columnsquery, null, null, null, null, null, null);
                        message.type = "JOINED";
                        cursor.moveToFirst();
                        String p1 = genHash(getPredecessor(message.senderPort, 1));
                        String p2 = genHash(getPredecessor(message.senderPort, 2));
                        for (cursor.moveToFirst(); !cursor.isAfterLast(); cursor.moveToNext()) {
                            String temp[] = new String[2];
                            temp[0] = cursor.getString(cursor.getColumnIndex("key"));
                            temp[1] = cursor.getString(cursor.getColumnIndex("value"));
                            String genHashKey = genHash(temp[0]);
                            if (message.firstSuccessor.equals(message.senderPort)) {
                                if (getPredecessor(message.senderPort, 1).equals("5562")) {
                                    if (genHashKey.compareTo("c25ddd596aa7c81fa12378fa725f706d54325d12") > 0 || genHashKey.compareTo("177ccecaec32c54b82d5aaafc18a2dadb753e3b1") <= 0) {
                                        message.keyValues.add(temp);
                                    }
                                } else {
                                    if (genHashKey.compareTo(p2) > 0 && genHashKey.compareTo(p1) <= 0) {
                                        message.keyValues.add(temp);
                                    }
                                }
                            } else {
                                if (message.senderPort.equals("5562")) {
                                    if (genHashKey.compareTo("c25ddd596aa7c81fa12378fa725f706d54325d12") > 0 || genHashKey.compareTo("177ccecaec32c54b82d5aaafc18a2dadb753e3b1") <= 0) {
                                        message.keyValues.add(temp);
                                    }
                                } else {
                                    if (genHashKey.compareTo(p1) > 0 && genHashKey.compareTo(message.hashedSenderPort) <= 0) {
                                        message.keyValues.add(temp);
                                    }
                                }

                            }
                        }
                        publishProgress(message);
                    } else if (message.type.equals("JOINED")) {
                        //waitingFlag = true;
                        SQLDatabase sqlDatabase = new SQLDatabase(getContext());
                        sqLiteDatabase = sqlDatabase.getWritableDatabase();
                        ContentValues values = new ContentValues();
                        sqLiteDatabase.beginTransaction();
                        for (String s[] : message.keyValues) {
                            values.put(SQLDatabase.KEY, s[0]);
                            values.put(SQLDatabase.VALUE, s[1]);

                            if (sqLiteDatabase.query(true, SQLDatabase.TABLE_NAME, columnsquery, "KEY='" + s[0] + "' and VALUE= '" + s[1] + "'", null, null, null, null, null).getCount() != 0) {
                            } else {
                                values.put(SQLDatabase.TIMESTAMP, System.currentTimeMillis());
                                sqLiteDatabase.insert(SQLDatabase.TABLE_NAME, null, values);
                            }
                        }
                        sqLiteDatabase.setTransactionSuccessful();
                        sqLiteDatabase.endTransaction();
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                    Log.e(TAG, "File write failed");
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }
        }

        protected void onProgressUpdate(Message... message) {
            try {
                if (message[0].type.equals("JOINED") || message[0].type.equals("null")) {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message[0]);
                }
            } catch (Exception e) {
                Log.e(TAG, "File write failed");
            }
        }

    }

    private class ClientTask extends AsyncTask<Message, Void, Object> {
        @Override
        protected Object doInBackground(Message... msgs) {
            String port = null;
            try {

                ObjectInputStream objectInputStream;
                Socket socket;
                if (msgs[0].type.equals("INSERT")) {
                    port = unHashedPort(msgs[0].hashedSenderPort);
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(unHashedPort(msgs[0].hashedSenderPort)) * 2);
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                    objectOutputStream.writeObject(msgs[0]);
                } else if (msgs[0].type.equals("JOIN")) {
                    for (String s : list) {
                        try {
                            if (s.equals(msgs[0].myPort) || s.equals(msgs[0].secondSuccessor)) {
                            } else {
                                msgs[0].senderPort = s;
                                msgs[0].hashedSenderPort = genHash(s);
                                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[0].senderPort) * 2);
                                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                                objectOutputStream.writeObject(msgs[0]);
                            }
                        } catch (IOException e) {
                            Log.e(TAG, "ClientTask socket IOException");
                        }
                    }
                    waitingFlag = false;
                } else if (msgs[0].type.equals("JOINED")) {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[0].myPort) * 2);
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                    objectOutputStream.writeObject(msgs[0]);
                } else if (msgs[0].type.equals("QUERY")) {
                    msgs[0].senderPort = unHashedPort(msgs[0].hashedSenderPort);
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[0].senderPort) * 2);
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                    objectOutputStream.writeObject(msgs[0]);
                    objectInputStream = new ObjectInputStream(socket.getInputStream());
                    return objectInputStream.readObject();
                } else if (msgs[0].type.equals("DELETE")) {
                    msgs[0].senderPort = unHashedPort(msgs[0].hashedSenderPort);
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[0].senderPort) * 2);
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                    objectOutputStream.writeObject(msgs[0]);
                }
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            return null;

        }
    }
}

class Message implements Serializable {

    String type;
    String senderPort;
    String hashedSenderPort;
    String hashedMyPort;
    String myPort;
    String key;
    String keyHash;
    String value;
    String firstSuccessor;
    String secondSuccessor;
    int index;
    ArrayList<String> list = new ArrayList<String>();
    ArrayList<String> hashedList = new ArrayList<String>();
    ArrayList<String[]> keyValues = new ArrayList<String[]>();
}

