package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by root on 4/10/17.
 */

public class SQLDatabase extends SQLiteOpenHelper {

    public static final String TABLE_NAME="SimpleDynamo";
    public static final String KEY="key";
    public static final String VALUE="value";
    public static final String TIMESTAMP="timestamp";
    public static final String CREATE_TABLE="CREATE TABLE "+ TABLE_NAME+" ("+KEY+" TEXT, "+VALUE+" TEXT, "+TIMESTAMP+" TEXT);";
    private static final int DATABASE_VERSION = 1;

    SQLDatabase(Context context) {
        super(context, TABLE_NAME, null, DATABASE_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL("DROP TABLE IF EXISTS "+TABLE_NAME);
        db.execSQL(CREATE_TABLE);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        if(oldVersion<newVersion)
        {
            db.execSQL("DROP TABLE IF EXISTS "+TABLE_NAME);
            db.execSQL(CREATE_TABLE);
        }
    }


}
