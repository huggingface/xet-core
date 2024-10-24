#[cfg(feature = "error_generic_member_access")]
pub mod structs {
    use std::backtrace::Backtrace;

    use xet_error::Error;

    #[derive(Error, Debug)]
    #[error("...")]
    pub struct OptSourceNoBacktrace {
        #[source]
        source: Option<anyhow::Error>,
    }

    #[derive(Error, Debug)]
    #[error("...")]
    pub struct OptSourceAlwaysBacktrace {
        #[source]
        source: Option<anyhow::Error>,
        backtrace: Backtrace,
    }

    #[derive(Error, Debug)]
    #[error("...")]
    pub struct NoSourceOptBacktrace {
        #[backtrace]
        backtrace: Option<Backtrace>,
    }

    #[derive(Error, Debug)]
    #[error("...")]
    pub struct AlwaysSourceOptBacktrace {
        source: anyhow::Error,
        #[backtrace]
        backtrace: Option<Backtrace>,
    }

    #[derive(Error, Debug)]
    #[error("...")]
    pub struct OptSourceOptBacktrace {
        #[source]
        source: Option<anyhow::Error>,
        #[backtrace]
        backtrace: Option<Backtrace>,
    }
}

#[cfg(feature = "error_generic_member_access")]
pub mod enums {
    use std::backtrace::Backtrace;

    use xet_error::Error;

    #[derive(Error, Debug)]
    pub enum OptSourceNoBacktrace {
        #[error("...")]
        Test {
            #[source]
            source: Option<anyhow::Error>,
        },
    }

    #[derive(Error, Debug)]
    pub enum OptSourceAlwaysBacktrace {
        #[error("...")]
        Test {
            #[source]
            source: Option<anyhow::Error>,
            backtrace: Backtrace,
        },
    }

    #[derive(Error, Debug)]
    pub enum NoSourceOptBacktrace {
        #[error("...")]
        Test {
            #[backtrace]
            backtrace: Option<Backtrace>,
        },
    }

    #[derive(Error, Debug)]
    pub enum AlwaysSourceOptBacktrace {
        #[error("...")]
        Test {
            source: anyhow::Error,
            #[backtrace]
            backtrace: Option<Backtrace>,
        },
    }

    #[derive(Error, Debug)]
    pub enum OptSourceOptBacktrace {
        #[error("...")]
        Test {
            #[source]
            source: Option<anyhow::Error>,
            #[backtrace]
            backtrace: Option<Backtrace>,
        },
    }
}

#[test]
#[cfg_attr(not(feature = "error_generic_member_access"), ignore)]
fn test_option() {}
