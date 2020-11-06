// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

pub use self::imp::wait_for_signal;

#[cfg(unix)]
mod imp {
    use engine_rocks::RocksEngine;
    use engine_traits::{Engines, MiscExt, RaftEngine};
    use libc::c_int;
    use nix::sys::signal::{SIGHUP, SIGINT, SIGTERM, SIGUSR1, SIGUSR2};
    use signal::trap::Trap;
    use std::fmt::Write;
    use std::fs::File;
    use std::{env, error, fmt};
    use tikv_util::metrics;

    fn fill_flamegraph(
        buffer: &mut String,
        trace: &mut String,
        snapshot: &tikv_alloc::trace::MemoryTrace,
    ) -> fmt::Result {
        let origin_len = trace.len();
        if trace.is_empty() {
            write!(trace, "{}", snapshot.id())?;
        } else {
            write!(trace, ";{}", snapshot.id())?;
        }
        let mut total = 0;
        append_readable_size(trace, snapshot.size());
        for c in snapshot.children() {
            fill_flamegraph(buffer, trace, c)?;
            total += c.size();
        }
        // When merging frame, size will of children will also be counted.
        if snapshot.size() > total {
            write!(buffer, "{} {}\n", trace, snapshot.size() - total)?;
        }
        trace.truncate(origin_len);
        Ok(())
    }

    fn append_readable_size(s: &mut String, size: usize) {
        let units = &["", "KiB", "MiB", "GiB"];
        let mut size = size as f64;
        for unit in units {
            if size > 1024.0 {
                size /= 1024.0;
                continue;
            }
            write!(s, "-{:.1}{}", size, unit).unwrap();
            return;
        }
        write!(s, " {:.1}TiB", size).unwrap();
    }

    fn generate_flamegraph(
        snapshot: &tikv_alloc::trace::MemoryTrace,
    ) -> Result<(), Box<dyn error::Error>> {
        let mut buffer = String::new();
        let mut trace = String::new();
        fill_flamegraph(&mut buffer, &mut trace, &snapshot)?;

        let path = env::temp_dir().join("tikv_memory.svg");
        let out = File::create(&path)?;
        info!("{}", buffer);
        inferno::flamegraph::from_lines(
            &mut inferno::flamegraph::Options::default(),
            buffer.lines(),
            out,
        )?;

        info!("memory flamegraph is generated at {}", path.display());
        Ok(())
    }

    #[allow(dead_code)]
    pub fn wait_for_signal<ER: RaftEngine>(engines: Option<Engines<RocksEngine, ER>>) {
        let trap = Trap::trap(&[SIGTERM, SIGINT, SIGHUP, SIGUSR1, SIGUSR2]);
        for sig in trap {
            match sig {
                SIGTERM | SIGINT | SIGHUP => {
                    info!("receive signal {}, stopping server...", sig as c_int);
                    break;
                }
                SIGUSR1 => {
                    // Use SIGUSR1 to log metrics.
                    info!("{}", metrics::dump());
                    if let Some(ref engines) = engines {
                        info!("{:?}", MiscExt::dump_stats(&engines.kv));
                        info!("{:?}", RaftEngine::dump_stats(&engines.raft));
                    }

                    if let Err(e) = generate_flamegraph(&tikv_alloc::trace::snapshot()) {
                        error!("failed to generate flamegraph: {:?}", e);
                    }
                }
                // TODO: handle more signal
                _ => unreachable!(),
            }
        }
    }
}

#[cfg(not(unix))]
mod imp {
    use engine_rocks::RocksEngine;
    use engine_traits::Engines;

    pub fn wait_for_signal(_: Option<Engines<RocksEngine, RocksEngine>>) {}
}
