#[cfg(feature = "debug")]
use {log::LevelFilter, std::io::Write};

#[cfg(feature = "debug")]
#[allow(dead_code)]
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
