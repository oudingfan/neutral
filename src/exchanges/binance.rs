use std::cmp::max;
use std::collections::{BTreeMap, HashMap};
use std::process::exit;
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::Sender;
use std::sync::{Arc, RwLock};
use std::thread::sleep;
use std::{fs, time};

use binance::account::OrderSide;
use binance::api::Binance as BinanceApi;
use binance::futures::account::{CustomOrderRequest, FuturesAccount, OrderType, TimeInForce};
use binance::futures::general::FuturesGeneral;
use binance::futures::market::FuturesMarket;
use binance::futures::userstream::FuturesUserStream;
use binance::futures::websockets::{
    FuturesMarket as FuturesMarketEnum, FuturesWebSockets, FuturesWebsocketEvent,
};
use binance::model::{Filters, KlineSummaries};
use chrono::{DateTime, NaiveDateTime, Utc};
use indicatif::ProgressIterator;
use num_traits::{FromPrimitive, ToPrimitive};
use rust_decimal::{Decimal, RoundingStrategy};
use serde_json::{json, Value};

use crate::exchanges::{Exchange, ExistingOrder, Order, OrderOperation, Side, SizeUnit};

const BINANCE_ORDERS_FILE: &str = "binance_orders.json";
const BINANCE_API_KEY: &str = "";
const BINANCE_API_SECRET: &str = "";

pub struct Binance {
    account: FuturesAccount,
    market: FuturesMarket,
    general: FuturesGeneral,
}

impl Binance {
    pub fn new() -> Self {
        Self {
            account: FuturesAccount::new(
                Some(BINANCE_API_KEY.into()),
                Some(BINANCE_API_SECRET.into()),
            ),
            market: FuturesMarket::new(None, None),
            general: FuturesGeneral::new(None, None),
        }
    }
}

impl Exchange for Binance {
    fn channel_name(&self) -> &str {
        "notify-binance"
    }

    fn get_symbols(
        &self,
        symbols_rank: u16,
    ) -> (Vec<String>, HashMap<String, u32>, HashMap<String, Decimal>) {
        let mut markets: BTreeMap<Decimal, String> = BTreeMap::new();
        let mut precisions_amount: HashMap<String, u32> = HashMap::new();
        let mut precisions_price: HashMap<String, Decimal> = HashMap::new();
        match self.general.exchange_info() {
            Ok(exchange_info) => {
                for symbol in exchange_info.symbols.iter().progress() {
                    precisions_amount
                        .insert(symbol.symbol.clone(), symbol.quantity_precision as u32);
                    for filter in symbol.filters.iter() {
                        if let Filters::PriceFilter {
                            min_price: _min_price,
                            max_price: _max_price,
                            tick_size,
                        } = filter
                        {
                            precisions_price.insert(
                                symbol.symbol.clone(),
                                Decimal::from_str(tick_size).unwrap().normalize(),
                            );
                        }
                    }
                    if !symbol.symbol.contains('_')
                        && symbol.status == "TRADING"
                        && symbol.quote_asset == "USDT"
                        && ![
                            //精确度过低，定期调整
                            "XEM", "BNX", "STMX", "CELO", "FLM", "CRV", "DGB", "FLOW", "LIT",
                            "DENT", "SUSHI", "OGN", "XTZ", "EOS", "LINA", "1000LUNC",
                            "RUNE", "TRB", "CTSI", "ALICE", "CHR", "ATA", "COCOS",
                            //成交率过低
                            "USDC",
                            //"ICX", "ROSE", "ZRX", "FTM", "HOT", "OMG",
                            //测试时，最小下单量过大
                            "BTC", "SOL", "AVAX", "QNT", "AXS", "YFI", "AAVE", "UNI",
                        ]
                        .contains(&symbol.base_asset.as_str())
                    {
                        let prices =
                            self.market
                                .get_klines(symbol.symbol.clone(), "1d", 5, None, None);
                        match prices {
                            Ok(KlineSummaries::AllKlineSummaries(candles)) => {
                                markets.insert(
                                    Decimal::from_str(
                                        &candles[candles.len() - 2].quote_asset_volume,
                                    )
                                    .unwrap(),
                                    symbol.symbol.clone(),
                                );
                            }
                            Err(e) => {
                                panic!("Error: {}", e);
                            }
                        }
                        sleep(time::Duration::from_millis(30));
                    }
                }
            }
            Err(e) => {
                println!("Error: {e}");
            }
        };
        for _i in 0..(markets.len() - symbols_rank as usize) {
            markets.pop_first();
        }
        (
            markets.into_values().collect(),
            precisions_amount,
            precisions_price,
        )
    }

    fn get_prices(
        &self,
        limit: u8,
        end_time: DateTime<Utc>,
        markets: &[String],
    ) -> HashMap<String, Vec<Decimal>> {
        let mut prices: HashMap<String, Vec<Decimal>>;
        let mut open_time: Option<i64>;
        let mut retry: bool;
        loop {
            prices = HashMap::new();
            open_time = None;
            retry = false;
            for symbol in markets.iter().progress() {
                let price = self.market.get_klines(
                    symbol,
                    "1m",
                    limit as u16,
                    None,
                    end_time.timestamp_millis() as u64,
                );
                match price {
                    Ok(KlineSummaries::AllKlineSummaries(price)) => {
                        let last_open_time = price.last().unwrap().open_time;
                        match open_time {
                            Some(time) => {
                                if time != last_open_time {
                                    println!(
                                        "时间不一致 {} {} {}",
                                        symbol,
                                        NaiveDateTime::from_timestamp_opt(time / 1000, 0).unwrap(),
                                        NaiveDateTime::from_timestamp_opt(last_open_time / 1000, 0)
                                            .unwrap()
                                    );
                                    retry = true;
                                    sleep(time::Duration::from_secs(1));
                                    break;
                                }
                            }
                            None => {
                                open_time = Some(last_open_time);
                            }
                        }
                        prices.insert(
                            symbol.clone(),
                            price
                                .iter()
                                .map(|p| Decimal::from_str(&p.close).unwrap())
                                .collect(),
                        );
                    }
                    Err(e) => {
                        println!("Error: {e}");
                        retry = true;
                        break;
                    }
                }
                sleep(time::Duration::from_millis(30));
            }
            if !retry {
                break;
            }
        }
        prices
    }

    fn get_balance(&self) -> Decimal {
        let mut balance: Decimal = Default::default();
        match self.account.account_information() {
            Ok(account) => {
                for asset in account.assets {
                    if asset.asset == "BUSD" {
                        balance += Decimal::from_f64(asset.margin_balance).unwrap();
                        break;
                    }
                    if asset.asset == "USDT" {
                        balance += Decimal::from_f64(asset.margin_balance).unwrap();
                        break;
                    }
                }
            }
            Err(e) => {
                println!("Error: {e}");
            }
        }
        balance
    }

    fn get_positions(&self) -> HashMap<String, Decimal> {
        let mut positions: HashMap<String, Decimal> = HashMap::new();
        match self.account.account_information() {
            Ok(account) => {
                for position in account.positions {
                    let d = Decimal::from_f64(position.position_amount).unwrap();
                    if !d.is_zero() {
                        positions.insert(position.symbol, d);
                    }
                }
            }
            Err(e) => {
                println!("Error: {e}");
            }
        }
        positions
    }

    fn get_open_orders(&self) -> HashMap<u64, ExistingOrder> {
        match serde_json::from_reader(match fs::File::open(BINANCE_ORDERS_FILE) {
            Ok(file) => file,
            Err(_) => fs::File::create(BINANCE_ORDERS_FILE).unwrap(),
        }) {
            Ok::<Vec<Order>, _>(json) => {
                let mut orders: HashMap<u64, ExistingOrder> = HashMap::new();
                for order in &json {
                    match self.account.get_all_open_orders(&order.symbol) {
                        Ok(open_orders) => {
                            for open_order in open_orders {
                                orders.insert(
                                    open_order.order_id,
                                    ExistingOrder {
                                        symbol: open_order.symbol.clone(),
                                        side: Side::from_str(&open_order.side).unwrap(),
                                        size: Decimal::from_f64(open_order.orig_qty).unwrap(),
                                        price: Decimal::from_f64(open_order.price).unwrap(),
                                        order_id: open_order.order_id,
                                        reduce_only: open_order.reduce_only,
                                        status: open_order.status,
                                        time: DateTime::from_utc(
                                            NaiveDateTime::from_timestamp_opt(
                                                (open_order.update_time / 1000) as i64,
                                                (open_order.update_time % 1000) as u32 * 1_000_000,
                                            )
                                            .unwrap(),
                                            Utc,
                                        ),
                                    },
                                );
                            }
                        }
                        Err(e) => {
                            println!("Error: {:?}", e);
                        }
                    }
                }
                println!("已有订单: {:#?}", orders);
                orders
            }
            Err(_) => HashMap::new(),
        }
    }

    fn place_orders(
        &self,
        orders: HashMap<String, Order>,
        existing_orders: &Arc<RwLock<HashMap<u64, ExistingOrder>>>,
        precisions_amount: &HashMap<String, u32>,
        precisions_price: &HashMap<String, Decimal>,
        chase: bool,
    ) -> Vec<Value> {
        for order in existing_orders.read().unwrap().values() {
            if !orders.contains_key(&order.symbol) || orders[&order.symbol].side != order.side {
                println!("取消订单 {}", order.symbol);
                self.cancel_order(&order.symbol, order.order_id);
            }
        }

        let mut results: Vec<Value> = Vec::new();

        for order in orders.values() {
            println!("{order:?}");

            let mut order_existed = false;
            for open_order in existing_orders.read().unwrap().values() {
                if open_order.symbol != order.symbol {
                    continue;
                } else if open_order.side != order.side {
                    self.cancel_order(&open_order.symbol, open_order.order_id);
                } else if chase {
                    if self.get_ticker_price(&order.symbol, &order.side) == open_order.price {
                        order_existed = true;
                        println!("订单已存在不再下单 {}", order.symbol);
                        break;
                    } else {
                        println!("不在第一档虽存在也取消订单 {}", order.symbol);
                        self.cancel_order(&open_order.symbol, open_order.order_id);
                    }
                } else {
                    if order_existed {
                        println!("取消重复订单 {} {}", open_order.order_id, open_order.symbol);
                        self.cancel_order(&open_order.symbol, open_order.order_id);
                    }
                    order_existed = true;
                    println!("订单已存在不再下单 {}", order.symbol);
                }
            }
            if order_existed {
                continue;
            }

            let price = self.get_ticker_price(&order.symbol, &order.side);
            let mut size = match order.size_type {
                SizeUnit::Usd => order.size / price,
                SizeUnit::Coin => order.size,
            };
            let price = match order.side {
                Side::Buy => {
                    price - precisions_price[&order.symbol] * Decimal::from(order.price_offset)
                }
                Side::Sell => {
                    price + precisions_price[&order.symbol] * Decimal::from(order.price_offset)
                }
            };
            size = size.round_dp_with_strategy(
                precisions_amount[&order.symbol],
                RoundingStrategy::AwayFromZero,
            );
            let side = match order.side {
                Side::Buy => OrderSide::Buy,
                Side::Sell => OrderSide::Sell,
            };
            let reduce_only = matches!(order.operation, OrderOperation::Close);
            let result = self.account.custom_order(CustomOrderRequest {
                symbol: order.symbol.clone(),
                side,
                position_side: None,
                order_type: OrderType::Limit,
                time_in_force: Some(TimeInForce::GTX),
                qty: size.to_f64(),
                reduce_only: Some(reduce_only),
                price: Some(price.to_f64().unwrap()),
                stop_price: None,
                close_position: None,
                activation_price: None,
                callback_rate: None,
                working_type: None,
                price_protect: None,
            });
            println!("下单结果 {result:?}");
            match result {
                Ok(trans) => {
                    results.push(json!(trans));
                }
                Err(e) => {
                    println!("下单出错 {e}");
                }
            }
            if orders.len() > 6 {
                sleep(time::Duration::from_millis(100));
            }
        }

        serde_json::to_writer(
            fs::File::create(BINANCE_ORDERS_FILE).unwrap(),
            &orders.values().collect::<Vec<&Order>>(),
        )
        .unwrap();

        results
    }
}

impl Binance {
    pub fn subscribe_candles(
        candles: Arc<RwLock<HashMap<String, BTreeMap<u64, Decimal>>>>,
        markets: Arc<RwLock<Vec<String>>>,
        limit: u8,
    ) {
        let futures_market = FuturesMarket::new(None, None);
        let kline: String = "!miniTicker@arr".to_string();
        let mut new_minute = 0;
        let mut web_socket = FuturesWebSockets::new(|event: FuturesWebsocketEvent| {
            if let FuturesWebsocketEvent::MiniTickerAll(tickers) = event {
                let mut candles = candles.write().unwrap();
                let markets = markets.read().unwrap();
                let timestamp_minute = Utc::now().timestamp() as u64 / 60 * 60;
                if timestamp_minute > new_minute {
                    println!("产生新一分钟 {}", timestamp_minute);
                    new_minute = timestamp_minute;
                    for (_, candle) in candles.iter_mut() {
                        candle.insert(timestamp_minute, *candle.last_key_value().unwrap().1);
                        while candle.len() > limit as usize {
                            candle.pop_first();
                        }
                    }
                }
                for ticker in tickers {
                    if !markets.contains(&ticker.symbol) {
                        if candles.contains_key(&ticker.symbol) {
                            candles.remove(&ticker.symbol);
                        }
                        continue;
                    }
                    if let Some(candle) = candles.get_mut(&ticker.symbol) {
                        candle.insert(
                            max(ticker.event_time / 1000 / 60 * 60, new_minute),
                            Decimal::from_str(&ticker.close).unwrap(),
                        );
                        while candle.len() > limit as usize {
                            candle.pop_first();
                        }
                    } else {
                        println!("新币种: {}", ticker.symbol);
                        let price = futures_market.get_klines(
                            &ticker.symbol,
                            "1m",
                            limit as u16,
                            None,
                            None,
                        );
                        match price {
                            Ok(KlineSummaries::AllKlineSummaries(prices)) => {
                                let mut candle = BTreeMap::new();
                                for price in prices {
                                    candle.insert(
                                        (price.open_time / 1000 / 60 * 60) as u64,
                                        Decimal::from_str(&price.close).unwrap(),
                                    );
                                }
                                candles.insert(ticker.symbol.clone(), candle);
                            }
                            Err(e) => {
                                println!("Error: {e}");
                                panic!("Error: {e}");
                            }
                        }
                        println!("{} 历史数据获取完毕 {}", ticker.symbol, candles.len());
                        sleep(time::Duration::from_millis(30));
                    }
                }
            };
            Ok(())
        });

        web_socket
            .connect(&FuturesMarketEnum::USDM, &kline)
            .unwrap();
        if let Err(e) = web_socket.event_loop(&AtomicBool::new(true)) {
            println!("Error: {:?}", e);
        }
        web_socket.disconnect().unwrap();
    }

    pub fn subscribe_account(
        positions: Arc<RwLock<HashMap<String, Decimal>>>,
        balance: Arc<RwLock<Decimal>>,
        orders: Arc<RwLock<HashMap<u64, ExistingOrder>>>,
        sender: Sender<String>,
    ) {
        let user_stream: FuturesUserStream = FuturesUserStream::new(
            Some(BINANCE_API_KEY.into()),
            Some(BINANCE_API_SECRET.into()),
        );

        if let Ok(answer) = user_stream.start() {
            let listen_key = answer.listen_key;

            let mut web_socket: FuturesWebSockets =
                FuturesWebSockets::new(|event: FuturesWebsocketEvent| {
                    match event {
                        FuturesWebsocketEvent::AccountUpdate(account_update) => {
                            let mut new_balance = Decimal::ZERO;
                            for b in &account_update.data.balances {
                                if b.asset == "USDT" || b.asset == "BUSD" {
                                    new_balance +=
                                        Decimal::from_str(&b.cross_wallet_balance).unwrap();
                                }
                            }
                            if new_balance != Decimal::ZERO {
                                *balance.write().unwrap() = new_balance;
                                println!("推送余额: {}", new_balance);
                            }
                            for position in &account_update.data.positions {
                                let d = Decimal::from_str(&position.position_amount).unwrap();
                                if d.is_zero() {
                                    positions.write().unwrap().remove(&position.symbol);
                                } else {
                                    positions
                                        .write()
                                        .unwrap()
                                        .insert(position.symbol.clone(), d);
                                }
                            }
                            println!("推送持仓 {} {positions:?}", positions.read().unwrap().len());
                        }
                        FuturesWebsocketEvent::OrderTrade(trade) => {
                            if vec!["FILLED", "CANCELED", "EXPIRED", "NEW_INSURANCE", "NEW_ADL"]
                                .contains(&trade.order.order_status.as_str())
                            {
                                orders.write().unwrap().remove(&trade.order.order_id);
                                sender
                                    .send(format!(
                                        "Symbol: {}, Side: {}, Price: {}, Execution Type: {}",
                                        trade.order.symbol,
                                        trade.order.side,
                                        trade.order.price,
                                        trade.order.order_status
                                    ))
                                    .unwrap();
                            } else {
                                orders.write().unwrap().insert(
                                    trade.order.order_id,
                                    ExistingOrder {
                                        order_id: trade.order.order_id,
                                        symbol: trade.order.symbol.clone(),
                                        side: Side::from_str(&trade.order.side).unwrap(),
                                        size: Decimal::from_str(&trade.order.qty).unwrap(),
                                        price: Decimal::from_str(&trade.order.price).unwrap(),
                                        reduce_only: trade.order.is_reduce_only,
                                        status: trade.order.order_status,
                                        time: DateTime::from_utc(
                                            NaiveDateTime::from_timestamp_opt(
                                                (trade.order.trade_order_time / 1000) as i64,
                                                (trade.order.trade_order_time % 1000) as u32
                                                    * 1_000_000,
                                            )
                                            .unwrap(),
                                            Utc,
                                        ),
                                    },
                                );
                            }
                            println!(
                                "推送挂单 {:?}",
                                orders
                                    .read()
                                    .unwrap()
                                    .values()
                                    .map(|o| o.symbol.clone())
                                    .collect::<Vec<String>>()
                            );
                        }
                        _ => (),
                    };
                    match user_stream.keep_alive(&listen_key) {
                        Ok(msg) => println!("Keepalive user data stream: {:?}", msg),
                        Err(e) => println!("Error: {:?}", e),
                    }
                    Ok(())
                });

            web_socket
                .connect(&FuturesMarketEnum::USDM, &listen_key)
                .unwrap();
            if let Err(e) = web_socket.event_loop(&AtomicBool::new(true)) {
                let err = e;
                {
                    println!("Error: {:?}", err);
                }
            }
        } else {
            println!("Not able to start an User Stream (Check your API_KEY)");
            exit(1);
        }
    }

    fn get_ticker_price(&self, symbol: &str, side: &Side) -> Decimal {
        let bid_ask = self.market.get_book_ticker(symbol).unwrap();
        Decimal::from_f64(match side {
            Side::Buy => bid_ask.bid_price,
            Side::Sell => bid_ask.ask_price,
        })
        .unwrap()
    }

    fn cancel_order(&self, symbol: &str, order_id: u64) {
        match self.account.cancel_order(symbol, order_id) {
            Ok(status) => {
                println!("取消成功 {} {} {}", symbol, order_id, status.status);
            }
            Err(e) => {
                println!("取消成功 {} {} {}", symbol, order_id, e);
            }
        }
    }
}
