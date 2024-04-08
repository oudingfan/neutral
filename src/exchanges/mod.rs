use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub mod binance;

#[derive(Clone, Debug, clap::ValueEnum)]
pub enum ExchangesName {
    Binance,
}

pub trait Exchange {
    fn channel_name(&self) -> &str;
    fn get_symbols(
        &self,
        symbols_rank: u16,
    ) -> (Vec<String>, HashMap<String, u32>, HashMap<String, Decimal>);
    fn get_prices(
        &self,
        limit: u8,
        end_time: DateTime<Utc>,
        markets: &[String],
    ) -> HashMap<String, Vec<Decimal>>;
    fn get_balance(&self) -> Decimal;
    fn get_positions(&self) -> HashMap<String, Decimal>;
    fn get_open_orders(&self) -> HashMap<u64, ExistingOrder>;
    fn place_orders(
        &self,
        orders: HashMap<String, Order>,
        existing_orders: &Arc<RwLock<HashMap<u64, ExistingOrder>>>,
        precisions_amount: &HashMap<String, u32>,
        precisions_price: &HashMap<String, Decimal>,
        chase: bool,
    ) -> Vec<Value>;
}

#[derive(Debug)]
pub struct OpenOrders {
    pub buy_orders: Vec<String>,
    pub sell_orders: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SizeUnit {
    Usd,
    Coin,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum OrderOperation {
    Open,
    Close,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Order {
    pub symbol: String,
    pub side: Side,
    pub size: Decimal,
    pub size_type: SizeUnit,
    pub price_offset: usize,
    pub operation: OrderOperation,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug)]
pub struct ExistingOrder {
    pub order_id: u64,
    pub symbol: String,
    pub side: Side,
    pub size: Decimal,
    pub price: Decimal,
    pub reduce_only: bool,
    pub status: String,
    pub time: DateTime<Utc>,
}

impl std::str::FromStr for Side {
    type Err = String;

    fn from_str(side: &str) -> Result<Self, Self::Err> {
        match side.to_lowercase().as_str() {
            "buy" => Ok(Side::Buy),
            "sell" => Ok(Side::Sell),
            _ => Err(format!("Invalid side: {side}")),
        }
    }
}
