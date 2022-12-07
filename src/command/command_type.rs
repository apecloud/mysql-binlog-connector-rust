#![allow(dead_code)]
pub enum CommandType {
    // Internal server command
    Sleep = 0,
    // Used to inform the server that client wants to close the connection
    Quit = 1,
    // Used to change the default schema of the connection
    InitDb = 2,
    // Used to send the server a text-based query that is executed immediately
    Query = 3,
    // Used to get column definitions of the specific table
    FieldList = 4,
    // Used to create new schema
    CreateDb = 5,
    // Used to drop existing schema
    DropDb = 6,
    // A low-level version of several FLUSH  and RESET  commands
    Refresh = 7,
    // Used to shutdown the mysql-server
    Shutdown = 8,
    // Used to get a human readable string of internal statistics
    Statistics = 9,
    // Used to get a list of active threads
    ProcessInfo = 10,
    // nternal server command
    Connect = 11,
    // Used to ask the server to terminate the connection
    ProcessKill = 12,
    // Triggers a dump on internal debug info to stdout of the mysql-server
    Debug = 13,
    // Used to check if the server is alive
    Ping = 14,
    // Internal server command
    Time = 15,
    // Internal server command
    DelayedInsert = 16,
    // Used to change user of the current connection and reset the connection state
    ChangeUser = 17,
    // Requests a binary log network stream from the master starting a given position
    BinlogDump = 18,
    // Used to dump a specific table
    TableDump = 19,
    // Internal server command
    ConnectOut = 20,
    // Registers a slave at the master Should be sent before requesting a binary log events with {@link #BINLOG_DUMP}
    RegisterSlave = 21,
    // Creates a prepared statement from the passed query string
    StmtPrepare = 22,
    // Used to execute a prepared statement as identified by statement id
    StmtExecute = 23,
    // Used to send some data for a column
    StmtSendLongData = 24,
    // Deallocates a prepared statement
    StmtClose = 25,
    // Resets the data of a prepared statement which was accumulated with {@link #STMT_SEND_LONG_DATA} commands
    StmtRest = 26,
    // Allows to enable and disable {@link comgithubshyikomysqlbinlognetworkClientCapabilities#MULTI_STATEMENTS}
    // for the current connection
    SetOption = 27,
    // Fetch a row from a existing resultset after a {@link #STMT_EXECUTE}
    StmtFetch = 28,
    // Internal server command
    Daemon = 29,
    // Used to request the binary log network stream based on a GTID
    BinlogDumpGtid = 30,
}
