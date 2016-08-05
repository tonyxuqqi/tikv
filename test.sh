#!/usr/bin/env bash
PREFIX=~/rust
sh ~/rust/lib/rustlib/uninstall.sh
sh ~/rust-installer/rustup.sh --date=2016-07-31 --prefix=$PREFIX --disable-sudo --channel=nightly
export PATH=$PREFIX/bin/:$PATH
export LD_LIBRARY_PATH=$PREFIX/lib:$LD_LIBRARY_PATH
ulimit -n 2000 && LOG_LEVEL=DEBUG RUST_BACKTRACE=1 cargo test --features ${ENABLE_FEATURES} -- --nocapture 

