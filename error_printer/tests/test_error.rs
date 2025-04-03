use error_printer::ErrorPrinter;
use tracing_test::traced_test;

#[test]
#[traced_test]
fn test_error() {
    let err: Result<(), &str> = Err("some error");
    // Important: the line number of this log statement
    // is important as we check the log output to make sure the file/line are correct.
    let line_num = 11;
    assert!(err.log_error("error test").is_err());

    check_logs(logs_contain, "ERROR", line_num)
}

#[test]
#[traced_test]
fn test_warn() {
    let err: Result<(), &str> = Err("some error");
    // Important: the line number of this log statement
    // is important as we check the log output to make sure the file/line are correct.
    let line_num = 23;
    assert!(err.warn_error("warn test").is_err());

    check_logs(logs_contain, "WARN", line_num)
}

#[test]
#[traced_test]
fn test_debug() {
    let err: Result<(), &str> = Err("some error");
    // Important: the line number of this log statement
    // is important as we check the log output to make sure the file/line are correct.
    let line_num = 35;
    assert!(err.debug_error("debug test").is_err());

    check_logs(logs_contain, "DEBUG", line_num)
}

#[test]
#[traced_test]
fn test_info() {
    let err: Result<(), &str> = Err("some error");
    // Important: the line number of this log statement
    // is important as we check the log output to make sure the file/line are correct.
    let line_num = 47;
    assert!(err.info_error("info test").is_err());

    check_logs(logs_contain, "INFO", line_num)
}

#[test]
fn test_ok() {
    let i = 2642;
    let res: Result<i32, ()> = Ok(i);
    assert_eq!(i, res.log_error("was err").unwrap());
    assert_eq!(i, res.warn_error("was err").unwrap());
    assert_eq!(i, res.debug_error("was err").unwrap());
    assert_eq!(i, res.info_error("was err").unwrap());
}

fn check_logs<F: Fn(&str) -> bool>(logs_contain: F, log_level: &str, line_num: i32) {
    assert!(logs_contain(log_level));
    assert!(logs_contain("test, error: \"some error\""));
    #[cfg(not(windows))]
    let expected_line = format!("{}:{}", file!(), line_num);
    #[cfg(windows)]
    let expected_line = format!("{}:{}", file!(), line_num).replace("\\", "\\\\");

    assert!(logs_contain(&expected_line));
}
