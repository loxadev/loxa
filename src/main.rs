fn main() {
    match loxa::run_from_env() {
        Ok(code) => std::process::exit(code),
        Err(error) => {
            loxa::report_error(&error);
            std::process::exit(1);
        }
    }
}
