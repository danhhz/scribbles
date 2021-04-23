// Copyright 2021 Daniel Harrison. All Rights Reserved.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{self, BufRead, Write};
use std::path::Path;

use flate2::write::GzEncoder;
use flate2::Compression;

use crate::ntriple::{Literal, NTriple, Object, ParseFile, Parser, Subject, TripleURI};

#[derive(Debug, Copy, Clone, PartialOrd, Ord, PartialEq, Eq, Hash)]
struct IDHash(u64);

#[derive(Debug, Copy, Clone, PartialOrd, Ord, PartialEq, Eq, Hash)]
struct BaseHash(u64);

fn hash(s: &str) -> u64 {
  let mut h = DefaultHasher::new();
  s.hash(&mut h);
  h.finish()
}

struct IDsByBase {
  id_to_base: HashMap<IDHash, BaseHash>,
  base_to_str: HashMap<BaseHash, String>,
  str_to_base: HashMap<String, BaseHash>,
}

pub fn sort_triples<P: AsRef<Path>>(
  ids_file: P,
  all_file: P,
  output_base: P,
) -> Result<(), Box<dyn Error>> {
  let mut ids_file = ParseFile::new(ids_file)?;
  let mut all_file = ParseFile::new(all_file)?;
  let output_base = output_base.as_ref();
  let mut output_by_base: HashMap<String, GzEncoder<File>> = HashMap::new();
  let output_fn = |base: &str, triple: &NTriple| -> io::Result<()> {
    if let Some(w) = output_by_base.get_mut(base) {
      writeln!(w, "{}", triple.raw())
    } else {
      let mut path = output_base.to_path_buf();
      path.push(base);
      path.set_extension("nt.gz");
      let w = File::create(&path).map_err(|err| {
        println!("creating {:?}: {}", &path, err);
        err
      })?;
      let mut w = GzEncoder::new(w, Compression::default());
      writeln!(w, "{}", triple.raw())?;
      output_by_base.insert(base.to_string(), w);
      Ok(())
    }
  };
  sort_triples_fn(&mut ids_file, &mut all_file, output_fn)?;
  let finished = output_by_base.drain().map(|(_, w)| w.finish());
  for finished in finished.map(|f_res| f_res.map(|f| f.sync_all())) {
    finished??;
  }
  Ok(())
}

fn sort_triples_fn<B: BufRead, F: FnMut(&str, &NTriple) -> io::Result<()>>(
  ids_file: &mut ParseFile<B>,
  all_file: &mut ParseFile<B>,
  mut output_fn: F,
) -> Result<(), Box<dyn Error>> {
  let ids_by_base = slurp_ids(ids_file)?;

  Parser::new().parse(all_file, |triple| {
    let base = match triple.sub() {
      Subject::URI(TripleURI::Freebase(x)) => {
        if x.starts_with("ns/m.") || x.starts_with("ns/g.") {
          match ids_by_base.id_to_base.get(&IDHash(hash(x))) {
            Some(base_hash) => match ids_by_base.base_to_str.get(base_hash) {
              Some(base) => base.as_str(),
              None => return Ok(()),
            },
            None => return Ok(()),
          }
        } else {
          let base = x.trim_start_matches("ns/").split('.').next().expect("internal error");
          base
        }
      }
      x => {
        eprintln!("{} sub={:?}", triple.raw(), x);
        return Ok(());
      }
    };

    let obj = triple.obj();
    match triple.pred() {
      TripleURI::W3(_) => return Ok(()),
      TripleURI::Freebase(x) => {
        if x == "ns/type.type.instance"
          || x.starts_with("key/base.")
          || x.starts_with("key/user.")
          || x.starts_with("ns/base.")
          || x.starts_with("ns/user.")
          || (x.starts_with("key/wikipedia.") && !x.starts_with("key/wikipedia.en"))
        {
          return Ok(());
        } else if x == "ns/type.object.key" {
          if let Object::Literal(Literal::Str(y)) = obj {
            if y.starts_with("/base/") || y.starts_with("/user/") {
              return Ok(());
            } else if y.starts_with("/wikipedia/") && !y.starts_with("/wikipedia/en") {
              return Ok(());
            }
          }
        } else if x == "ns/common.topic.topic_equivalent_webpage" {
          if let Object::URI(TripleURI::Absolute(y)) = obj {
            if y.contains(".wikipedia.org/") && !y.contains("en.wikipedia.org/") {
              return Ok(());
            }
          }
        }
      }
      _ => {}
    }
    match obj {
      Object::Literal(Literal::Lang { literal: _, lang }) => {
        if lang != "en" {
          return Ok(());
        }
      }
      Object::URI(TripleURI::Freebase(x)) => {
        if x.starts_with("ns/base") || x.starts_with("ns/user") {
          return Ok(());
        }
      }
      _ => {}
    }

    output_fn(base, &triple)?;
    Ok(())
  })?;
  Ok(())
}

fn slurp_ids<B: BufRead>(ids_file: &mut ParseFile<B>) -> Result<IDsByBase, Box<dyn Error>> {
  let parser = Parser::new();

  let mut ret = IDsByBase {
    id_to_base: HashMap::new(),
    base_to_str: HashMap::new(),
    str_to_base: HashMap::new(),
  };

  parser.parse(ids_file, |triple| {
    match triple.pred() {
      TripleURI::Freebase("ns/kg.object_profile.prominent_type") => {}
      _ => return Ok(()),
    }
    let id_hash = match triple.sub() {
      Subject::URI(TripleURI::Freebase(x)) => {
        if !(x.starts_with("ns/m.") || x.starts_with("ns/g.")) {
          return Ok(());
        }
        IDHash(hash(x))
      }
      _ => return Ok(()),
    };
    let base_hash = match triple.obj() {
      Object::URI(TripleURI::Freebase(x)) => {
        let base = x.trim_start_matches("ns/").split('.').next().expect("internal error");
        if base == "base" || base == "user" {
          return Ok(());
        }
        let base_hash = ret.str_to_base.get(base).map(|base| *base);
        base_hash.unwrap_or_else(|| {
          let base_hash = BaseHash(hash(base));
          ret.str_to_base.insert(base.to_string(), base_hash);
          ret.base_to_str.insert(base_hash, base.to_string());
          base_hash
        })
      }
      _ => return Ok(()),
    };
    ret.id_to_base.insert(id_hash, base_hash);
    Ok(())
  })?;

  Ok(ret)
}

#[cfg(test)]
mod test {
  use std::error::Error;
  use std::io::{self};

  use crate::ntriple::{NTriple, ParseFile};

  #[test]
  fn sort_triples() -> Result<(), Box<dyn Error>> {
    // TODO: Tests for the freebase data dump specific modifications to the
    // grammar.
    let triples = r#"
      # All triples for an ID with a prominent_type triple are kept (mod below
      # filtering) and output under the "base" (first part of "book.book").
      <http://rdf.freebase.com/ns/m.1>  <http://rdf.freebase.com/ns/type.object.name> "One"@en .
      <http://rdf.freebase.com/ns/m.1>  <http://rdf.freebase.com/ns/kg.object_profile.prominent_type> <http://rdf.freebase.com/ns/book.book>  .
      <http://rdf.freebase.com/ns/m.2>  <http://rdf.freebase.com/ns/type.object.name> "Two"@en .
      <http://rdf.freebase.com/ns/m.2>  <http://rdf.freebase.com/ns/kg.object_profile.prominent_type> <http://rdf.freebase.com/ns/film.film>  .
      # IDs without a prominent_type are filtered.
      <http://rdf.freebase.com/ns/m.3>  <http://rdf.freebase.com/ns/type.object.name> "Three"@en-gb  .
      # Non-english names are filtered.
      <http://rdf.freebase.com/ns/m.1>  <http://rdf.freebase.com/ns/type.object.name> "One GB"@en-gb  .
      # Objects in ns/base and ns/user are filtered.
      <http://rdf.freebase.com/ns/m.1>  <http://rdf.freebase.com/ns/type.object.type> <http://rdf.freebase.com/ns/base/foo> .
      <http://rdf.freebase.com/ns/m.1>  <http://rdf.freebase.com/ns/type.object.type> <http://rdf.freebase.com/ns/user/foo> .
      # Predicates in {key,ns}/{base/user} are filtered.
      <http://rdf.freebase.com/ns/m.1>  <http://rdf.freebase.com/ns/base.foo> <http://rdf.freebase.com/ns/book.book> .
      <http://rdf.freebase.com/ns/m.1>  <http://rdf.freebase.com/ns/user.foo> <http://rdf.freebase.com/ns/book.book> .
      <http://rdf.freebase.com/ns/m.1>  <http://rdf.freebase.com/key/base.foo> <http://rdf.freebase.com/ns/book.book> .
      <http://rdf.freebase.com/ns/m.1>  <http://rdf.freebase.com/key/user.foo> <http://rdf.freebase.com/ns/book.book> .
      # type.object.key to /base and /user and filtered.
      <http://rdf.freebase.com/ns/m.1>  <http://rdf.freebase.com/ns/type.object.key> "/base/one" .
      <http://rdf.freebase.com/ns/m.1>  <http://rdf.freebase.com/ns/type.object.key> "/user/one" .
      # type.object.key to English Wikipedia is kept but non-English Wikipedia is filtered.
      <http://rdf.freebase.com/ns/m.1>  <http://rdf.freebase.com/ns/type.object.key> "/wikipedia/en/One" .
      <http://rdf.freebase.com/ns/m.1>  <http://rdf.freebase.com/ns/type.object.key> "/wikipedia/fr/Un" .
      # Other type.object.keys are kept.
      <http://rdf.freebase.com/ns/m.1>  <http://rdf.freebase.com/ns/type.object.key> "/imdb/one" .
      # topic_equivalent_webpage to English Wikipedia is kept but non-English Wikipedia is filtered.
      <http://rdf.freebase.com/ns/m.1>  <http://rdf.freebase.com/ns/common.topic.topic_equivalent_webpage> <http://en.wikipedia.org/wiki/One> .
      <http://rdf.freebase.com/ns/m.1>  <http://rdf.freebase.com/ns/common.topic.topic_equivalent_webpage> <http://fr.wikipedia.org/wiki/Un> .
      # topic_equivalent_webpage to non-Wikipedia is kept.
      <http://rdf.freebase.com/ns/m.1>  <http://rdf.freebase.com/ns/common.topic.topic_equivalent_webpage> <http://one.com> .
    "#.trim();
    let mut ids_file =
      ParseFile { buf: triples.as_bytes(), size: triples.len() as u64, gzipped: false };
    let mut all_file =
      ParseFile { buf: triples.as_bytes(), size: triples.len() as u64, gzipped: false };

    let mut output = Vec::new();
    let output_fn = |base: &str, triple: &NTriple| -> io::Result<()> {
      output.push(format!("{}: {}", base, triple.raw().trim()));
      Ok(())
    };
    super::sort_triples_fn(&mut ids_file, &mut all_file, output_fn)?;

    let expected = vec![
      r#"book: <http://rdf.freebase.com/ns/m.1>  <http://rdf.freebase.com/ns/common.topic.topic_equivalent_webpage> <http://en.wikipedia.org/wiki/One> ."#.to_string(),
      r#"book: <http://rdf.freebase.com/ns/m.1>  <http://rdf.freebase.com/ns/common.topic.topic_equivalent_webpage> <http://one.com> ."#.to_string(),
      r#"book: <http://rdf.freebase.com/ns/m.1>  <http://rdf.freebase.com/ns/kg.object_profile.prominent_type> <http://rdf.freebase.com/ns/book.book>  ."#.to_string(),
      r#"book: <http://rdf.freebase.com/ns/m.1>  <http://rdf.freebase.com/ns/type.object.key> "/imdb/one" ."#.to_string(),
      r#"book: <http://rdf.freebase.com/ns/m.1>  <http://rdf.freebase.com/ns/type.object.key> "/wikipedia/en/One" ."#.to_string(),
      r#"book: <http://rdf.freebase.com/ns/m.1>  <http://rdf.freebase.com/ns/type.object.name> "One"@en ."#.to_string(),
      r#"film: <http://rdf.freebase.com/ns/m.2>  <http://rdf.freebase.com/ns/kg.object_profile.prominent_type> <http://rdf.freebase.com/ns/film.film>  ."#.to_string(),
      r#"film: <http://rdf.freebase.com/ns/m.2>  <http://rdf.freebase.com/ns/type.object.name> "Two"@en ."#.to_string(),
    ];
    output.sort();

    assert_eq!(expected, output);
    Ok(())
  }
}
