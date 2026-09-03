use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "devices")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub relay_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub endpoint: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub device_id: String,
    pub last_acked_sequence: i64,
    #[sea_orm(indexed)]
    pub last_seen_at: i64,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
