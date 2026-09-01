use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "counters")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub relay_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub destination: String,
    pub next_sequence: i64,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
