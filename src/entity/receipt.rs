use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "receipts")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub relay_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub source: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub message_id: String,
    pub destination: String,
    pub sequence: i64,
    pub payload: Json,
    #[sea_orm(indexed)]
    pub created_at: i64,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
