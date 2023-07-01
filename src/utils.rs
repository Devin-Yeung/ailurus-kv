#[cfg(feature = "debug")]
use {log::LevelFilter, std::io::Write};

#[cfg(feature = "debug")]
pub(crate) fn logging() {
    env_logger::Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                "{}:{} [{}] - {}",
                record.file().unwrap_or("unknown"),
                record.line().unwrap_or(0),
                record.level(),
                record.args()
            )
        })
        .filter(None, LevelFilter::Debug)
        .init();
}

#[macro_export]
macro_rules! ecast {
    ($err:expr) => {{
        #[cfg(feature = "debug")]
        {
            $err.map_err(|e| e.downcast::<$crate::errors::Errors>().unwrap())
        }
        #[cfg(not(feature = "debug"))]
        {
            $err
        }
    }};
}
