package edu.buffalo.cse.cse486586.simpledht;

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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.List;

public class SimpleDhtProvider extends ContentProvider {

    static final int SERVER_PORT = 10000;
    static final String TAG = SimpleDhtActivity.class.getSimpleName();
    ArrayList<String> hashedList = new ArrayList<String>();
    ArrayList<String> list = new ArrayList<String>();
    SQLiteDatabase sqLiteDatabase;
    String[] columns = {"KEY", "VALUE"};
    String portStr = null;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        Message message = new Message();
        if (list.size() > 1) {
            Socket socket = null;
            if (selection.equals("\"*\"") || selection.equals("*")) {
                message.key = null;
                message.type = "*DELETE";
                for (String s : list) {
                    try {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(s) * 2);
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                        objectOutputStream.writeObject(message);
                    } catch (IOException e) {
                        e.printStackTrace();
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
                            message.hashedSenderPort = hashedList.get(i + 1);
                            message.type = "DELETE";
                            message.key = selection;
                            message.keyHash = genHash(selection);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
                        } else if (genHash(selection).compareTo(hashedList.get(hashedList.size() - 1)) > 0 || genHash(selection).compareTo(hashedList.get(0)) == 0 ||
                                genHash(selection).compareTo(hashedList.get(0)) < 0) {
                            message.hashedSenderPort = hashedList.get(0);
                            message.type = "DELETE";
                            message.key = selection;
                            message.keyHash = genHash(selection);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
                        }
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }
                }
            }
        } else {
            if (selection.equals("\"*\"") || selection.equals("\"@\"") || selection.equals("*") || selection.equals("@")) {
                SQLDatabase sqlDatabase = new SQLDatabase(getContext());
                SQLiteDatabase sqLiteDatabase = sqlDatabase.getWritableDatabase();
                sqLiteDatabase.delete(SQLDatabase.TABLE_NAME, null, null);
            } else {
                SQLDatabase sqlDatabase = new SQLDatabase(getContext());
                SQLiteDatabase sqLiteDatabase = sqlDatabase.getWritableDatabase();
                sqLiteDatabase.delete(SQLDatabase.TABLE_NAME, "KEY='" + selection + "'", null);
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
        try {
            String key = values.getAsString("key");
            String value = values.getAsString("value");
            String keyHash = genHash(key);
            Message message = new Message();
            message.keyHash = keyHash;
            message.key = key;
            message.value = value;
            message.type = "INSERT";
            if (list.size() > 1) {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
            } else {
                values.put(SQLDatabase.KEY, message.key);
                values.put(SQLDatabase.VALUE, message.value);
                sqLiteDatabase.insert(SQLDatabase.TABLE_NAME, null, values);
            }
            Log.v("insert", values.toString());
        } catch (Exception e) {
            Log.e("Insert", "File write failed");
        }
        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        String node_id = null;
        SQLDatabase sqlDatabase = new SQLDatabase(getContext());
        sqLiteDatabase = sqlDatabase.getWritableDatabase();
        try {
            TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
            portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            node_id = genHash(portStr);

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            if (!portStr.equals("5554")) {
                Message message = new Message();
                message.senderPort = portStr;
                message.hashedSenderPort = node_id;
                message.type = "JOIN";
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
            } else if (portStr.equals("5554")) {
                list.add("5554");
                hashedList.add(genHash("5554"));
            }
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        MatrixCursor cursor = new MatrixCursor(columns);
        Message message = new Message();
        if (list.size() > 1) {
            if (selection.equals("\"*\"") || selection.equals("*")) {
                Socket socket ;
                message.list = list;
                message.hashedList = hashedList;
                message.key = null;
                message.type = "*QUERY";
                List<String[]> newList = new ArrayList<String[]>();

                try {
                    for (String s : message.list) {
                        message.senderPort = s;
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(message.senderPort) * 2);
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                        objectOutputStream.writeObject(message);
                        ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
                        List<String[]> list1;
                        list1 = (ArrayList<String[]>) objectInputStream.readObject();
                        newList.addAll(list1);


                    }
                } catch (Exception e) {
                }
                for (String[] s : newList) {
                    cursor.addRow(new Object[]{s[0], s[1]});
                }

                return cursor;
            } else if (selection.equals("\"@\"") || selection.equals("@")) {
                SQLDatabase sqlDatabase = new SQLDatabase(getContext());
                SQLiteDatabase sqLiteDatabase = sqlDatabase.getReadableDatabase();
                return sqLiteDatabase.query(false, SQLDatabase.TABLE_NAME, columns, null, null, null, null, null, null);
            } else {
                try {
                    for (int i = 0; i < hashedList.size() - 1; i++) {
                        if (genHash(selection).compareTo(hashedList.get(i)) > 0 && genHash(selection).compareTo(hashedList.get(i + 1)) <= 0) {
                            message.hashedSenderPort = hashedList.get(i + 1);
                            message.type = "QUERY";
                            message.key = selection;
                            message.keyHash = genHash(selection);
                            ArrayList<String[]> keyValues = (ArrayList<String[]>) new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message).get();
                            for (String[] s : keyValues) {
                                cursor.addRow(new Object[]{s[0], s[1]});
                            }
                            return cursor;
                        } else if (genHash(selection).compareTo(hashedList.get(hashedList.size() - 1)) > 0 || genHash(selection).compareTo(hashedList.get(0)) == 0 ||
                                genHash(selection).compareTo(hashedList.get(0)) < 0) {
                            message.hashedSenderPort = hashedList.get(0);
                            message.type = "QUERY";
                            message.key = selection;
                            message.keyHash = genHash(selection);
                            ArrayList<String[]> keyValues = (ArrayList<String[]>) new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message).get();
                            for (String[] s : keyValues) {
                                cursor.addRow(new Object[]{s[0], s[1]});
                            }
                            return cursor;

                        }
                    }
                } catch (Exception e) {

                }
            }
        } else {

            if (selection.equals("\"*\"") || selection.equals("\"@\"") || selection.equals("*") || selection.equals("@")) {
                SQLDatabase sqlDatabase = new SQLDatabase(getContext());
                SQLiteDatabase sqLiteDatabase = sqlDatabase.getReadableDatabase();
                return sqLiteDatabase.query(false, SQLDatabase.TABLE_NAME, columns, null, null, null, null, null, null);
            } else {
                SQLDatabase sqlDatabase = new SQLDatabase(getContext());
                SQLiteDatabase sqLiteDatabase = sqlDatabase.getReadableDatabase();
                return sqLiteDatabase.query(false, SQLDatabase.TABLE_NAME, columns, "KEY='" + selection + "'", null, null, null, null, null);
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
                    if (message.type.equals("JOIN")) {
                        list.add(message.senderPort);
                        hashedList.add(genHash(message.senderPort));
                        message.type = "JOINED";
                        message.list = (ArrayList<String>) list.clone();
                        message.hashedList = (ArrayList<String>) hashedList.clone();
                        publishProgress(message);
                    }else if (message.type.equals("JOINED")) {
                        list = message.list;
                        hashedList = message.hashedList;
                        Collections.sort(hashedList);
                    }  else if (message.type.equals("INSERT")) {
                        for (int i = 0; i < hashedList.size() - 1; i++) {
                            if (message.keyHash.compareTo(hashedList.get(i)) > 0 && message.keyHash.compareTo(hashedList.get(i + 1)) <= 0) {
                                message.hashedSenderPort = hashedList.get(i + 1);
                                message.type = "INSERT REPLY";
                                publishProgress(message);
                            } else if (message.keyHash.compareTo(hashedList.get(hashedList.size() - 1)) > 0 || message.keyHash.compareTo(hashedList.get(0)) == 0 ||
                                    message.keyHash.compareTo(hashedList.get(0)) < 0) {
                                message.hashedSenderPort = hashedList.get(0);
                                message.type = "INSERT REPLY";
                                publishProgress(message);
                            }
                        }
                    } else if (message.type.equals("INSERT REPLY")) {
                        ContentValues values = new ContentValues();
                        values.put(SQLDatabase.KEY, message.key);
                        values.put(SQLDatabase.VALUE, message.value);
                        sqLiteDatabase.insert(SQLDatabase.TABLE_NAME, null, values);
                        //sqLiteDatabase.close();
                    } else if (message.type.equals("QUERY") || message.type.equals("*QUERY")) {
                        SQLDatabase sqlDatabase = new SQLDatabase(getContext());
                        SQLiteDatabase sqLiteDatabase = sqlDatabase.getReadableDatabase();

                        Cursor cursor;
                        if (message.type.equals("QUERY")) {
                            cursor = sqLiteDatabase.query(true, SQLDatabase.TABLE_NAME, columns, "KEY='" + message.key + "'", null, null, null, null, null);
                        } else {
                            cursor = sqLiteDatabase.query(true, SQLDatabase.TABLE_NAME, columns, null, null, null, null, null, null);

                        }
                        ArrayList<String[]> keyValues = new ArrayList<String[]>();
                        cursor.moveToFirst();
                        for (cursor.moveToFirst(); !cursor.isAfterLast(); cursor.moveToNext()) {
                            String temp[] = new String[2];
                            temp[0] = cursor.getString(cursor.getColumnIndex("key"));
                            temp[1] = cursor.getString(cursor.getColumnIndex("value"));
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
                if (message[0].type.equals("INSERT REPLY"))
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message[0]);
                else if (message[0].type.equals("JOINED")) {
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
            try {
                ObjectInputStream objectInputStream;
                Socket socket;
                if (msgs[0].type.equals("JOIN") || msgs[0].type.equals("INSERT")) {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt("11108"));
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                    objectOutputStream.writeObject(msgs[0]);
                } else if (msgs[0].type.equals("JOINED")) {
                    for (String s : msgs[0].list) {
                        msgs[0].senderPort = s;
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[0].senderPort) * 2);
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                        objectOutputStream.writeObject(msgs[0]);
                    }
                } else if (msgs[0].type.equals("INSERT REPLY")) {
                    msgs[0].senderPort = unHashedPort(msgs[0].hashedSenderPort);
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[0].senderPort) * 2);
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

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            return null;

        }
    }
}

class Message implements Serializable {

    String senderPort;
    String type;
    String hashedSenderPort;
    String key;
    String keyHash;
    String value;
    ArrayList<String> list = new ArrayList<String>();
    ArrayList<String> hashedList = new ArrayList<String>();
}
