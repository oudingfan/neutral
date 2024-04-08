use std::collections::{BTreeMap, HashMap};
use std::process::exit;
use std::sync::{mpsc, Arc, RwLock};
use std::thread;
use std::thread::sleep;

use chrono::{Duration, DurationRound, Timelike, Utc};
use clap::Parser;
use num_traits::{One, Zero};
use rust_decimal::Decimal;

use crate::exchanges::binance::Binance;
use crate::exchanges::{
    Exchange, ExchangesName, ExistingOrder, Order, OrderOperation, Side, SizeUnit,
};

#[derive(Parser, Debug)]
pub struct StrategyConfig {
    #[arg(long)]
    exchange: ExchangesName,

    #[arg(long)]
    frequency: u8,

    #[arg(long)]
    max_positions: usize,

    #[arg(long)]
    reduce_only: bool,

    #[arg(long)]
    roll: bool,

    #[arg(long)]
    chase: bool,

    #[arg(long)]
    symbols_rank: u16,
}

pub struct Strategy {
    reduce_only: bool,
    roll: bool,
    positions: usize,
    frequency: u8,
    look_back: u8,
    chase: bool,
    symbols_rank: u16,
    api: Box<dyn Exchange>,
}

impl Strategy {
    pub fn new(config: StrategyConfig) -> Box<Self> {
        let api: Box<dyn Exchange> = match config.exchange {
            ExchangesName::Binance => Box::new(Binance::new()),
        };
        Box::new(Strategy {
            reduce_only: config.reduce_only,
            roll: config.roll,
            positions: config.max_positions,
            frequency: config.frequency,
            look_back: 5,
            chase: config.chase,
            symbols_rank: config.symbols_rank,
            api,
        })
    }

    pub fn run(&self) {
        let (markets, mut precisions_amount, mut precisions_price) =
            self.api.get_symbols(self.symbols_rank);
        println!("市场 {markets:?}");
        let mut updated_daily = Utc::now();

        let markets: Arc<RwLock<Vec<String>>> = Arc::new(RwLock::new(markets));
        let prices: Arc<RwLock<HashMap<String, BTreeMap<u64, Decimal>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let positions: Arc<RwLock<HashMap<String, Decimal>>> =
            Arc::new(RwLock::new(self.api.get_positions()));
        let balance: Arc<RwLock<Decimal>> = Arc::new(RwLock::new(self.api.get_balance()));
        let existing_orders: Arc<RwLock<HashMap<u64, ExistingOrder>>> =
            Arc::new(RwLock::new(self.api.get_open_orders()));

        if self.reduce_only {
            self.reduce_only_run(
                &positions,
                &existing_orders,
                &precisions_amount,
                &precisions_price,
            );
        }

        let prices_subscriber = Arc::clone(&prices);
        let markets_subscriber = Arc::clone(&markets);
        let limit_subscriber = self.look_back;
        let positions_subscriber = Arc::clone(&positions);
        let balance_subscriber = Arc::clone(&balance);
        let orders_subscriber = Arc::clone(&existing_orders);
        let (sender, receiver) = mpsc::channel();

        thread::spawn(move || {
            Binance::subscribe_candles(prices_subscriber, markets_subscriber, limit_subscriber)
        });
        thread::spawn(move || {
            Binance::subscribe_account(
                positions_subscriber,
                balance_subscriber,
                orders_subscriber,
                sender,
            )
        });

        loop {
            if Utc::now().date_naive() != updated_daily.date_naive() && Utc::now().minute() == 2 {
                updated_daily = Utc::now();
                let (a, b, c) = self.api.get_symbols(self.symbols_rank);
                let mut markets_main = markets.write().unwrap();
                markets_main.clear();
                markets_main.extend(a);
                drop(markets_main);
                precisions_amount = b;
                precisions_price = c;
                println!("市场 {markets:?}");
            }

            let prices_data = prices.read().unwrap();
            if prices_data.len() < self.symbols_rank as usize {
                drop(prices_data);
                println!("等待价格数据");
                sleep(Duration::seconds(1).to_std().unwrap());
                continue;
            }
            let factors = self.calculate_bias(&prices_data);
            drop(prices_data);

            let balance = balance.read().unwrap();
            let positions = positions.read().unwrap();
            let orders = match self.roll {
                true => {
                    Strategy::calculate_roll_orders(self.positions, &balance, &factors, &positions)
                }
                false => Strategy::calculate_orders(self.positions, &balance, &factors, &positions),
            };
            Strategy::print_positions_ranking(&positions, &factors);
            drop(positions);
            drop(balance);
            println!("生成订单 {} {orders:?}", orders.len());

            self.api.place_orders(
                orders,
                &existing_orders,
                &precisions_amount,
                &precisions_price,
                self.chase,
            );

            if self.roll {
                let now = Utc::now();
                let mut next = now.duration_trunc(Duration::minutes(1)).unwrap()
                    + Duration::minutes(
                        (self.frequency as u32 - now.minute() % self.frequency as u32) as i64,
                    )
                    - Duration::seconds(10);
                if next < now {
                    next += Duration::minutes(self.frequency as i64);
                }
                println!("等待至 {next}");
                sleep((next - now).to_std().unwrap());
                continue;
            } else {
                match receiver.recv_timeout(Duration::seconds(3).to_std().unwrap()) {
                    Ok(data) => {
                        println!("订单推送 {data:?}");
                        for d in receiver.try_iter() {
                            println!("订单推送 {d:?}");
                        }
                    }
                    Err(_) => {
                        println!("等待超时");
                    }
                }
            }
        }
    }

    fn reduce_only_run(
        &self,
        positions: &Arc<RwLock<HashMap<String, Decimal>>>,
        existing_orders: &Arc<RwLock<HashMap<u64, ExistingOrder>>>,
        precisions_amount: &HashMap<String, u32>,
        precisions_price: &HashMap<String, Decimal>,
    ) {
        let orders = Strategy::calculate_reduce_orders(&positions.read().unwrap());
        println!("生成订单 {} {orders:?}", orders.len());
        let result = self.api.place_orders(
            orders,
            existing_orders,
            precisions_amount,
            precisions_price,
            true,
        );
        println!("下单结果 {result:?}");
        exit(0);
    }

    fn calculate_bias(
        &self,
        prices: &HashMap<String, BTreeMap<u64, Decimal>>,
    ) -> BTreeMap<Decimal, String> {
        let mut biases: BTreeMap<Decimal, String> = BTreeMap::new();
        for (symbol, price) in prices {
            let mut sum = Decimal::zero();
            for (_, p) in price.iter().rev().take(self.look_back as usize) {
                sum += p;
            }
            let mean = sum / Decimal::from(self.look_back);
            let bias = price.values().last().unwrap() / mean - Decimal::one();
            // if bias.abs() > Decimal::from_str("0.003").unwrap() {
            //     println!("偏离 {symbol} {bias} 过大");
            //     bias = Decimal::zero();
            // }
            biases.insert(bias, symbol.to_string());
        }
        biases
    }

    fn print_positions_ranking(
        positions: &HashMap<String, Decimal>,
        factors: &BTreeMap<Decimal, String>,
    ) {
        let mut positions_ranking: BTreeMap<isize, String> = BTreeMap::new();
        for (market, _) in positions.iter().filter(|(_, v)| v.is_sign_positive()) {
            if let Some(ranking) = factors.values().position(|m| m == market) {
                positions_ranking.insert(ranking as isize + 1, market.clone());
            }
        }
        for (market, _) in positions.iter().filter(|(_, v)| v.is_sign_negative()) {
            if let Some(ranking) = factors.values().rev().position(|m| m == market) {
                positions_ranking.insert(-(ranking as isize + 1), market.clone());
            }
        }
        println!("持仓排名 {} {positions_ranking:?}", positions_ranking.len());
    }

    fn calculate_reduce_orders(positions: &HashMap<String, Decimal>) -> HashMap<String, Order> {
        let mut orders: HashMap<String, Order> = HashMap::new();
        positions.keys().for_each(|symbol| {
            Strategy::insert_close_orders(&mut orders, positions, symbol, 0);
        });
        orders
    }

    fn calculate_roll_orders(
        max_positions: usize,
        balance: &Decimal,
        regressions: &BTreeMap<Decimal, String>,
        positions: &HashMap<String, Decimal>,
    ) -> HashMap<String, Order> {
        let long_symbols = regressions.values().take(max_positions).collect::<Vec<_>>();
        let short_symbols = regressions
            .values()
            .rev()
            .take(max_positions)
            .collect::<Vec<_>>();
        let mut orders = HashMap::new();
        let balance_part = balance / Decimal::from(max_positions * 2);
        for (symbol, position) in positions {
            if !((long_symbols.contains(&symbol) && position.is_sign_positive())
                || (short_symbols.contains(&symbol) && position.is_sign_negative()))
            {
                Strategy::insert_close_orders(&mut orders, positions, symbol, 0);
            }
        }

        for symbol in long_symbols {
            if !positions.contains_key(symbol) || positions[symbol].is_sign_negative() {
                Strategy::insert_open_orders(
                    &mut orders,
                    symbol,
                    Side::Buy,
                    if !positions.contains_key(symbol) {
                        balance_part
                    } else {
                        balance_part + balance_part
                    },
                    0,
                );
            }
        }

        for symbol in short_symbols {
            if !positions.contains_key(symbol) || positions[symbol].is_sign_positive() {
                Strategy::insert_open_orders(
                    &mut orders,
                    symbol,
                    Side::Sell,
                    if !positions.contains_key(symbol) {
                        balance_part
                    } else {
                        balance_part + balance_part
                    },
                    0,
                );
            }
        }

        orders
    }

    fn calculate_orders(
        max_positions: usize,
        balance: &Decimal,
        factors: &BTreeMap<Decimal, String>,
        positions: &HashMap<String, Decimal>,
    ) -> HashMap<String, Order> {
        let mut orders: HashMap<String, Order> = HashMap::new();

        for (symbol, position) in positions {
            if !factors.values().any(|m| m == symbol) {
                println!("删除无效持仓 {symbol:?} {position:}");
                Strategy::insert_close_orders(&mut orders, positions, symbol, 0);
            }
        }

        let long_symbols = factors.values().rev().collect::<Vec<_>>();
        let short_symbols = factors.values().collect::<Vec<_>>();
        let symbols_length = factors.len();

        let long_positions = positions
            .iter()
            .filter(|(_, v)| v.is_sign_positive())
            .map(|v| v.0)
            .collect::<Vec<_>>();
        let short_positions = positions
            .iter()
            .filter(|(_, v)| v.is_sign_negative())
            .map(|v| v.0)
            .collect::<Vec<_>>();
        let mut positive = long_positions.len();
        let mut negative = short_positions.len();
        let imbalance = positive as isize - negative as isize;

        let part: Decimal = balance / Decimal::from(max_positions * 2);

        // 平多
        let mut close_long_inserted = 0;
        for (i, &symbol) in long_symbols.iter().enumerate() {
            if (close_long_inserted == 0 && i < (symbols_length - (max_positions + 5)))
                || (close_long_inserted == 1 && i < (symbols_length - (max_positions + 5) - 5))
            {
                if long_positions.contains(&symbol) {
                    println!("L-{} {symbol}", symbols_length - i);
                    Strategy::insert_close_orders(&mut orders, positions, symbol, imbalance);
                    positive -= 1;
                    close_long_inserted += 1;
                }
            } else {
                break;
            }
        }

        // 平空
        let mut close_short_inserted = 0;
        for (i, &symbol) in short_symbols.iter().enumerate() {
            if (close_short_inserted == 0 && i < (symbols_length - (max_positions + 5)))
                || (close_short_inserted == 1 && i < (symbols_length - (max_positions + 5) - 5))
            {
                if short_positions.contains(&symbol) {
                    println!("S-{} {symbol}", symbols_length - i);
                    Strategy::insert_close_orders(&mut orders, positions, symbol, imbalance);
                    negative -= 1;
                    close_short_inserted += 1;
                }
            } else {
                break;
            }
        }

        // 开多
        let mut open_long_inserted = 0;
        for (i, &symbol) in short_symbols.iter().enumerate() {
            if (positive < negative) || (open_long_inserted <= 1 && positive < max_positions) {
                if !positions.contains_key(symbol) {
                    println!("L+{} {symbol}", i + 1);
                    Strategy::insert_open_orders(&mut orders, symbol, Side::Buy, part, imbalance);
                    positive += 1;
                    open_long_inserted += 1;
                }
            } else {
                break;
            }
        }

        // 开空
        for (i, &symbol) in long_symbols.iter().enumerate() {
            if negative < positive {
                if !positions.contains_key(symbol) {
                    println!("S+{} {symbol}", i + 1);
                    Strategy::insert_open_orders(&mut orders, symbol, Side::Sell, part, imbalance);
                    negative += 1;
                }
            } else {
                break;
            }
        }

        orders
    }

    fn insert_close_orders(
        orders: &mut HashMap<String, Order>,
        positions: &HashMap<String, Decimal>,
        symbol: &str,
        imbalance: isize,
    ) {
        let size = positions[symbol];
        orders.insert(
            symbol.to_string(),
            Order {
                symbol: symbol.to_string(),
                side: if size.is_sign_positive() {
                    Side::Sell
                } else {
                    Side::Buy
                },
                size: size.abs(),
                size_type: SizeUnit::Coin,
                price_offset: if size.is_sign_positive() == (imbalance < 0) {
                    imbalance.abs()
                } else {
                    0
                } as usize,
                operation: OrderOperation::Close,
            },
        );
    }

    fn insert_open_orders(
        orders: &mut HashMap<String, Order>,
        symbol: &str,
        side: Side,
        part: Decimal,
        imbalance: isize,
    ) {
        orders.insert(
            symbol.to_string(),
            Order {
                symbol: symbol.to_string(),
                side: side.clone(),
                size: part,
                size_type: SizeUnit::Usd,
                operation: OrderOperation::Open,
                price_offset: if (side == Side::Buy) == (imbalance > 0) {
                    imbalance.abs()
                } else {
                    0
                } as usize,
            },
        );
    }
}
