// Copyright 2021 Daniel Harrison. All Rights Reserved.

#![warn(unsafe_code)]
#![warn(clippy::correctness, clippy::perf, clippy::wildcard_imports)]
#![allow(clippy::if_same_then_else)]

use std::env;
use std::process;

mod ntriple;
mod partition_triples;

fn main() {
  // TODO: Usage errors
  let args = env::args_os().collect::<Vec<_>>();
  let ret = partition_triples::sort_triples(&args[1], &args[2], &args[3]);
  if let Err(err) = ret {
    eprintln!("error: {:?}", err);
    process::exit(1)
  }
}
