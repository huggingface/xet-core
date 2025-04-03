use error_printer::OptionPrinter;
use tracing_test::traced_test;

#[test]
#[traced_test]
fn test_error() {
    let opt: Option<()> = None;
    // Important: the line number of this log statement
    // is important as we check the log output to make sure the file/line are correct.
    let line_num = 11;
    assert!(opt.error_none("error test: opt is None").is_none());

    check_logs(logs_contain, "ERROR", line_num)
}

#[test]
#[traced_test]
fn test_warn() {
    let opt: Option<()> = None;
    // Important: the line number of this log statement
    // is important as we check the log output to make sure the file/line are correct.
    let line_num = 23;
    assert!(opt.warn_none("warn test: opt is None").is_none());

    check_logs(logs_contain, "WARN", line_num)
}

#[test]
#[traced_test]
fn test_debug() {
    let opt: Option<()> = None;
    // Important: the line number of this log statement
    // is important as we check the log output to make sure the file/line are correct.
    let line_num = 35;
    assert!(opt.debug_none("debug test: opt is None").is_none());

    check_logs(logs_contain, "DEBUG", line_num)
}

#[test]
#[traced_test]
fn test_info() {
    let opt: Option<()> = None;
    // Important: the line number of this log statement
    // is important as we check the log output to make sure the file/line are correct.
    let line_num = 47;
    assert!(opt.info_none("info test: opt is None").is_none());

    check_logs(logs_contain, "INFO", line_num)
}

#[test]
fn test_some() {
    let i = 2642;
    let opt = Some(i);
    assert_eq!(i, opt.error_none("was none").unwrap());
    assert_eq!(i, opt.warn_none("was none").unwrap());
    assert_eq!(i, opt.debug_none("was none").unwrap());
    assert_eq!(i, opt.info_none("was none").unwrap());
}

fn check_logs<F: Fn(&str) -> bool>(logs_contain: F, log_level: &str, line_num: i32) {
    assert!(logs_contain(log_level));
    assert!(logs_contain("opt is None"));
    #[cfg(not(windows))]
    let expected_line = format!("{}:{}", file!(), line_num);
    #[cfg(windows)]
    let expected_line = format!("{}:{}", file!(), line_num).replace("\\", "\\\\");
    assert!(logs_contain(&expected_line));
}
