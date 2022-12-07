use num_enum::{IntoPrimitive, TryFromPrimitive};

#[derive(IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
pub enum EventType {
    #[num_enum(default)]
    Unkown = 0,
    StartV3 = 1,
    Query = 2,
    Stop = 3,
    Rotate = 4,
    Intvar = 5,
    Load = 6,
    Slave = 7,
    CreateFile = 8,
    AppendBlock = 9,
    ExecLoad = 10,
    DeleteFile = 11,
    NewLoad = 12,
    Rand = 13,
    UserVar = 14,
    FormatDescription = 15,
    Xid = 16,
    BeginLoadQuery = 17,
    ExecuteLoadQuery = 18,
    TableMap = 19,
    PreGaWriteRows = 20,
    PreGaUpdateRows = 21,
    PreGaDeleteRows = 22,
    WriteRows = 23,
    UpdateRows = 24,
    DeleteRows = 25,
    Incident = 26,
    HeartBeat = 27,
    Ignorable = 28,
    RowsQuery = 29,
    ExtWriteRows = 30,
    ExtUpdateRows = 31,
    ExtDeleteRows = 32,
    Gtid = 33,
    AnonymousGtid = 34,
    PreviousGtid = 35,
    TransactionContext = 36,
    ViewChage = 37,
    XaPrepare = 38,
    PartialUpdateRowsEvent = 39,
    TransactionPayload = 40,
    AnnotateRows = 160,
    BinlogCheckpoint = 161,
    MariadbGtid = 162,
    MariadbGtidList = 163,
}

impl EventType {
    pub fn from_code(code: u8) -> EventType {
        EventType::try_from(code).unwrap()
    }

    pub fn to_code(event_type: EventType) -> u8 {
        event_type.into()
    }
}
