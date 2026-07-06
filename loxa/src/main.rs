use clap::Parser;

#[derive(Parser)]
#[command(name = "loxa", version, about = "Measured local AI infrastructure")]
struct Cli {}

fn main() {
    let _cli = Cli::parse();
}
