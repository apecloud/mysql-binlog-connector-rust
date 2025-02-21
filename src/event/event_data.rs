use serde::{Deserialize, Serialize};

use super::{
    delete_rows_event::DeleteRowsEvent, format_description_event::FormatDescriptionEvent,
    gtid_event::GtidEvent, previous_gtids_event::PreviousGtidsEvent, query_event::QueryEvent,
    rotate_event::RotateEvent, rows_query_event::RowsQueryEvent, table_map_event::TableMapEvent,
    transaction_payload_event::TransactionPayloadEvent, update_rows_event::UpdateRowsEvent,
    write_rows_event::WriteRowsEvent, xa_prepare_event::XaPrepareEvent, xid_event::XidEvent,
};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub enum EventData {
    NotSupported,
    FormatDescription(FormatDescriptionEvent),
    PreviousGtids(PreviousGtidsEvent),
    Gtid(GtidEvent),
    Query(QueryEvent),
    TableMap(TableMapEvent),
    WriteRows(WriteRowsEvent),
    UpdateRows(UpdateRowsEvent),
    DeleteRows(DeleteRowsEvent),
    Xid(XidEvent),
    XaPrepare(XaPrepareEvent),
    Rotate(RotateEvent),
    TransactionPayload(TransactionPayloadEvent),
    RowsQuery(RowsQueryEvent),
    HeartBeat,
}
