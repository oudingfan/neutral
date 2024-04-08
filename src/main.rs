use clap::Parser;

use crate::strategy::Strategy;
use crate::strategy::StrategyConfig;

mod exchanges;
mod strategy;

fn main() {
    let strategy_config = StrategyConfig::parse();
    println!("策略配置：{:?}", strategy_config);
    Strategy::new(strategy_config).run();
}
