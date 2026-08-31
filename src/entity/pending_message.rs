use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "pending_messages")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub relay_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub destination: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub sequence: i64,
    pub message_id: String,
    pub payload: Json,
    pub payload_bytes: i64,
    #[sea_orm(indexed)]
    pub created_at: i64,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
