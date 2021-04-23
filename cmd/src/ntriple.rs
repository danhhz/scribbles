// Copyright 2021 Daniel Harrison. All Rights Reserved.

#![allow(dead_code, unused_macros)]

use std::collections::HashMap;
use std::ffi::OsStr;
use std::fmt;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::Path;
use std::time::{Duration, Instant};

use flate2::bufread::MultiGzDecoder;
use regex::{CaptureLocations, Regex};

/// A regex for parsing the N Triple format.
///
/// This parses the N Triple format as described WIP
#[rustfmt::skip]
mod n_triples {
  macro_rules! WS {() => {concat!(r"[ \t]")}}
  macro_rules! CHARACTER {() => {concat!("[\x20-\x7E]")}}
  macro_rules! FB_URI {($name:expr) => {concat!("http://rdf.freebase.com/(?P<", $name, "_fb>", CHARACTER!(), "+)")}}
  macro_rules! W3_URI {($name:expr) => {concat!("http://www.w3.org/(?P<", $name, "_w3>", CHARACTER!(), "+)")}}
  macro_rules! ABSOLUTE_URI {($name:expr) => {concat!("(?P<", $name, ">.+)")}}
  macro_rules! URIREF {($name:expr) => {concat!("<(?:", FB_URI!($name), "|", W3_URI!($name), "|", ABSOLUTE_URI!($name), ")>")}}
  macro_rules! NAME {() => {concat!("[A-Za-z][A-Za-z0-9]*")}}
  macro_rules! NODE_ID {($name:expr) => {concat!("_:(?P<", $name, ">", NAME!(), ")")}}
  macro_rules! STRING {() => {concat!(CHARACTER!(), "+")}}
  macro_rules! LANGUAGE {() => {"[a-z]+(-[a-zA-Z0-9]+)*"}}
  macro_rules! LANG_STRING {($name:expr) => {concat!("\"(?P<", $name, "_lang_lit>.*)\"@(?P<", $name, "_lang_type>", LANGUAGE!(), ")")}}
  macro_rules! DATATYPE_STRING {($name:expr) => {concat!("\"(?P<", $name, "_data_lit>.*)\"\\^\\^", URIREF!(concat!($name, "_data_type")))}}
  macro_rules! LITERAL_STRING {($name:expr) => {concat!("\"(?P<", $name, "_str_lit>.*)\"")}}
  macro_rules! LITERAL {($name:expr) => {concat!(LANG_STRING!($name), "|", DATATYPE_STRING!($name), "|", LITERAL_STRING!($name))}}
  macro_rules! SUBJECT {() => {concat!("(?:", URIREF!("sub_uri"), "|", NODE_ID!("sub_node"), ")")}}
  macro_rules! PREDICATE {() => {URIREF!("prd_uri")}}
  macro_rules! OBJECT {() => {concat!("(?:", URIREF!("obj_uri"), "|", NODE_ID!("obj_node"), "|", LITERAL!("obj"),")")}}
  macro_rules! TRIPLE {() => {concat!(SUBJECT!(), WS!(), "+", PREDICATE!(), WS!(), "+", OBJECT!(), WS!(), "*", r"\.", WS!(), "*")}}
  macro_rules! COMMENT {() => {concat!("(?P<comment>#", CHARACTER!(), "*)")}}
  macro_rules! LINE {() => {concat!("^", WS!(), "*(?:", COMMENT!(), "|", TRIPLE!(), "|", "(?P<blank>))$")}}
  pub const LINE: &'static str = LINE!();
}

#[derive(Debug)]
pub enum TripleURI<'a> {
  Freebase(&'a str),
  W3(&'a str),
  Absolute(&'a str),
}

#[derive(Debug)]
pub enum Literal<'a> {
  Lang { literal: &'a str, lang: &'a str },
  Data { literal: &'a str, uri: Option<TripleURI<'a>> },
  Str(&'a str),
}

#[derive(Debug)]
pub enum Subject<'a> {
  URI(TripleURI<'a>),
  Node(&'a str),
}

#[derive(Debug)]
pub enum Object<'a> {
  URI(TripleURI<'a>),
  Node(&'a str),
  Literal(Literal<'a>),
}

pub struct NTriple<'a>(&'a CaptureLocations, &'a CaptureNames, &'a str);

impl<'a> fmt::Debug for NTriple<'a> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("NTriple")
      .field("sub", &self.sub())
      .field("pred", &self.pred())
      .field("obj", &self.obj())
      .finish()
  }
}

impl<'a> NTriple<'a> {
  pub fn raw(&self) -> &'a str {
    let NTriple(_, _, raw) = self;
    raw
  }

  pub fn sub(&self) -> Subject<'a> {
    let NTriple(locs, names, raw) = self;

    if let Some((start, end)) = locs.get(names.sub_uri_fb) {
      Subject::URI(TripleURI::Freebase(&raw[start..end]))
    } else if let Some((start, end)) = locs.get(names.sub_uri_w3) {
      Subject::URI(TripleURI::W3(&raw[start..end]))
    } else if let Some((start, end)) = locs.get(names.sub_uri) {
      Subject::URI(TripleURI::Absolute(&raw[start..end]))
    } else if let Some((start, end)) = locs.get(names.sub_node) {
      Subject::Node(&raw[start..end])
    } else {
      unreachable!(raw)
    }
  }

  pub fn pred(&self) -> TripleURI<'a> {
    let NTriple(locs, names, raw) = self;

    if let Some((start, end)) = locs.get(names.prd_uri_fb) {
      TripleURI::Freebase(&raw[start..end])
    } else if let Some((start, end)) = locs.get(names.prd_uri_w3) {
      TripleURI::W3(&raw[start..end])
    } else if let Some((start, end)) = locs.get(names.prd_uri) {
      TripleURI::Absolute(&raw[start..end])
    } else {
      unreachable!(raw)
    }
  }

  pub fn obj(&self) -> Object<'a> {
    let NTriple(locs, names, raw) = self;

    if let Some((start, end)) = locs.get(names.obj_uri_fb) {
      Object::URI(TripleURI::Freebase(&raw[start..end]))
    } else if let Some((start, end)) = locs.get(names.obj_uri_w3) {
      Object::URI(TripleURI::W3(&raw[start..end]))
    } else if let Some((start, end)) = locs.get(names.obj_uri) {
      Object::URI(TripleURI::Absolute(&raw[start..end]))
    } else if let Some((start, end)) = locs.get(names.obj_node) {
      Object::Node(&raw[start..end])
    } else if let Some((lit_start, lit_end)) = locs.get(names.obj_lang_lit) {
      let (lang_start, lang_end) = locs.get(names.obj_lang_type).expect("internal logic error");
      Object::Literal(Literal::Lang {
        literal: &raw[lit_start..lit_end],
        lang: &raw[lang_start..lang_end],
      })
    } else if let Some((start, end)) = locs.get(names.obj_data_lit) {
      let uri = if let Some(obj_data_type_fb) = locs.get(names.obj_data_type_fb) {
        Some(TripleURI::Freebase(&raw[obj_data_type_fb.0..obj_data_type_fb.1]))
      } else if let Some((start, end)) = locs.get(names.obj_data_type_w3) {
        Some(TripleURI::W3(&raw[start..end]))
      } else if let Some((start, end)) = locs.get(names.obj_data_type) {
        Some(TripleURI::Absolute(&raw[start..end]))
      } else {
        None
      };
      Object::Literal(Literal::Data { literal: &raw[start..end], uri: uri })
    } else if let Some((start, end)) = locs.get(names.obj_str_lit) {
      Object::Literal(Literal::Str(&raw[start..end]))
    } else {
      unreachable!(raw)
    }
  }
}

struct CaptureNames {
  sub_uri_fb: usize,
  sub_uri_w3: usize,
  sub_uri: usize,
  sub_node: usize,
  prd_uri_fb: usize,
  prd_uri_w3: usize,
  prd_uri: usize,
  obj_uri_fb: usize,
  obj_uri_w3: usize,
  obj_uri: usize,
  obj_node: usize,
  obj_lang_lit: usize,
  obj_lang_type: usize,
  obj_data_lit: usize,
  obj_data_type_fb: usize,
  obj_data_type_w3: usize,
  obj_data_type: usize,
  obj_str_lit: usize,
  comment: usize,
  blank: usize,
}

pub struct ParseFile<B: BufRead> {
  pub buf: B,
  pub size: u64,
  pub gzipped: bool,
}

impl ParseFile<BufReader<File>> {
  pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
    let path = path.as_ref();
    let mut f = File::open(path).map_err(|err| {
      println!("opening {:?}: {}", path, err);
      err
    })?;

    if path.extension() == Some(OsStr::new("gz")) {
      let mut buf = [0u8; 4];
      f.seek(SeekFrom::End(-1 * buf.len() as i64))?;
      f.read_exact(&mut buf)?;
      f.seek(SeekFrom::Start(0))?;
      let mut uncompressed_size = u64::from(u32::from_le_bytes(buf));
      let compressed_size = f.metadata()?.len();
      // Gzip's uncompressed size field is 4 bytes so it's wrong for files >
      // 4GB. The only one of those I'm dealing with is the full freebase triple
      // dump, so special case it.
      if compressed_size == 32_191_685_487 && uncompressed_size == 1_933_388_246 {
        uncompressed_size = 422_098_255_249;
      }
      Ok(ParseFile { buf: BufReader::new(f), size: uncompressed_size, gzipped: true })
    } else {
      let size = f.metadata()?.len();
      Ok(ParseFile { buf: BufReader::new(f), size: size, gzipped: false })
    }
  }
}

pub struct Parser {
  line_re: Regex,
  capture_names: CaptureNames,
  update_interval: Duration,
}

impl Parser {
  pub fn new() -> Parser {
    let line_re = Regex::new(n_triples::LINE).expect("internal error");
    let capture_idx_by_name: HashMap<_, _> =
      line_re.capture_names().enumerate().flat_map(|(idx, name)| name.map(|n| (n, idx))).collect();
    let capture_names = CaptureNames {
      sub_uri_fb: *capture_idx_by_name.get("sub_uri_fb").expect("internal error"),
      sub_uri_w3: *capture_idx_by_name.get("sub_uri_w3").expect("internal error"),
      sub_uri: *capture_idx_by_name.get("sub_uri").expect("internal error"),
      sub_node: *capture_idx_by_name.get("sub_node").expect("internal error"),
      prd_uri_fb: *capture_idx_by_name.get("prd_uri_fb").expect("internal error"),
      prd_uri_w3: *capture_idx_by_name.get("prd_uri_w3").expect("internal error"),
      prd_uri: *capture_idx_by_name.get("prd_uri").expect("internal error"),
      obj_uri_fb: *capture_idx_by_name.get("obj_uri_fb").expect("internal error"),
      obj_uri_w3: *capture_idx_by_name.get("obj_uri_w3").expect("internal error"),
      obj_uri: *capture_idx_by_name.get("obj_uri").expect("internal error"),
      obj_node: *capture_idx_by_name.get("obj_node").expect("internal error"),
      obj_lang_lit: *capture_idx_by_name.get("obj_lang_lit").expect("internal error"),
      obj_lang_type: *capture_idx_by_name.get("obj_lang_type").expect("internal error"),
      obj_data_lit: *capture_idx_by_name.get("obj_data_lit").expect("internal error"),
      obj_data_type_fb: *capture_idx_by_name.get("obj_data_type_fb").expect("internal error"),
      obj_data_type_w3: *capture_idx_by_name.get("obj_data_type_w3").expect("internal error"),
      obj_data_type: *capture_idx_by_name.get("obj_data_type").expect("internal error"),
      obj_str_lit: *capture_idx_by_name.get("obj_str_lit").expect("internal error"),
      comment: *capture_idx_by_name.get("comment").expect("internal error"),
      blank: *capture_idx_by_name.get("blank").expect("internal error"),
    };
    Parser {
      line_re: line_re,
      capture_names: capture_names,
      update_interval: Duration::from_secs(5),
    }
  }

  fn parse_buf<B: BufRead, F>(&self, buf: &mut B, buf_size: u64, mut triple_fn: F) -> io::Result<()>
  where
    F: FnMut(&NTriple<'_>) -> io::Result<()>,
  {
    let start = Instant::now();
    let mut updated = start;
    let mut processed = 0;
    let size_width = format!("{}", buf_size).len();

    let mut line = String::new();
    let mut capture_locs = self.line_re.capture_locations();

    loop {
      line.clear();
      if buf.read_line(&mut line)? == 0 {
        let runtime = Instant::now().saturating_duration_since(start);
        eprintln!("success! {}s: {}b", runtime.as_secs(), processed);
        return Ok(());
      }
      if line.ends_with('\n') {
        line.pop();
        if line.ends_with('\r') {
          line.pop();
        }
      }
      processed += line.len();
      let now = Instant::now();
      if now.saturating_duration_since(updated) >= self.update_interval {
        updated = now;
        let running = now.saturating_duration_since(start);
        let runtime_estimate = running.mul_f64(buf_size as f64 / processed as f64);
        eprintln!(
          "{:>4}/{:>4}s: {:>width$}/{:>width$}b",
          running.as_secs(),
          runtime_estimate.as_secs(),
          processed,
          buf_size,
          width = size_width,
        );
      }
      if let Some(_) = self.line_re.captures_read(&mut capture_locs, &line) {
        if capture_locs.get(self.capture_names.blank).is_some()
          || capture_locs.get(self.capture_names.comment).is_some()
        {
          continue;
        }
        triple_fn(&NTriple(&capture_locs, &self.capture_names, &line))?;
      } else {
        eprintln!("did not match: {}", line);
      }
    }
  }

  pub fn parse<B: BufRead, F>(&self, file: &mut ParseFile<B>, triple_fn: F) -> io::Result<()>
  where
    F: FnMut(&NTriple<'_>) -> io::Result<()>,
  {
    if file.gzipped {
      let mut buf = BufReader::new(MultiGzDecoder::new(&mut file.buf));
      self.parse_buf(&mut buf, file.size, triple_fn)
    } else {
      self.parse_buf(&mut file.buf, file.size, triple_fn)
    }
  }
}

#[cfg(test)]
mod test {
  use std::error::Error;

  use super::*;

  #[test]
  fn w3_golden() -> Result<(), Box<dyn Error>> {
    let buf = include_str!("w3_golden.nt");
    let mut file = ParseFile { buf: buf.as_bytes(), size: buf.len() as u64, gzipped: false };
    Parser::new().parse(&mut file, |_| Ok(()))?;
    Ok(())
  }
}
