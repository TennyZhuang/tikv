// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Implements SQL `LIKE`.
//!
//! This implementation needs refactor.
//!
//! 1. It is not effective. Consider target = 'aaaaaaaaaaaaaaa' and pattern = 'a%a%a%a%a%a%b'.
//!    See https://research.swtch.com/glob
//!
//! 2. It should support non-binary mode (and binary mode) and do case insensitive comparing
//!    in non-binary mode.

use std::slice::Iter;

use crate::expr::Result;

const MAX_RECURSE_LEVEL: usize = 1024;

// Do match until '%' is found.
#[inline]
fn partial_like(tcs: &mut Iter<'_, u8>, pcs: &mut Iter<'_, u8>, escape: u32) -> Option<bool> {
    loop {
        match pcs.next().cloned() {
            None => return Some(tcs.next().is_none()),
            Some(b'%') => return None,
            Some(c) => {
                let (npc, escape) = if u32::from(c) == escape {
                    pcs.next().map_or((c, false), |&c| (c, true))
                } else {
                    (c, false)
                };
                let nsc = match tcs.next() {
                    None => return Some(false),
                    Some(&c) => c,
                };
                if nsc != npc && (npc != b'_' || escape) {
                    return Some(false);
                }
            }
        }
    }
}

pub fn like(target: &[u8], pattern: &[u8], escape: u32, recurse_level: usize) -> Result<bool> {
    let mut tcs = target.iter();
    let mut pcs = pattern.iter();
    loop {
        if let Some(res) = partial_like(&mut tcs, &mut pcs, escape) {
            return Ok(res);
        }
        let next_char = loop {
            match pcs.next().cloned() {
                Some(b'%') => {}
                Some(b'_') => {
                    if tcs.next().is_none() {
                        return Ok(false);
                    }
                }
                // So the pattern should be some thing like 'xxx%'
                None => return Ok(true),
                Some(c) => {
                    break if u32::from(c) == escape {
                        pcs.next().map_or(escape, |&c| u32::from(c))
                    } else {
                        u32::from(c)
                    };
                }
            }
        };
        if recurse_level >= MAX_RECURSE_LEVEL {
            // TODO: maybe we should test if stack is actually about to overflow.
            return Err(box_err!(
                "recurse level should not be larger than {}",
                MAX_RECURSE_LEVEL
            ));
        }
        // Pattern must be something like "%xxx".
        loop {
            let s = match tcs.next() {
                None => return Ok(false),
                Some(&s) => u32::from(s),
            };
            if s == next_char && like(tcs.as_slice(), pcs.as_slice(), escape, recurse_level + 1)? {
                return Ok(true);
            }
        }
    }
}

pub fn like_regexp(target: &[u8], pattern: &[u8], escape: u32, _recurse_level: usize) -> Result<bool> {
    let mut pcs = pattern.iter();
    let mut res = String::from("^");
    loop {
        match pcs.next().cloned() {
            Some(b'%') => res.push_str(".*"),
            Some(b'_') => res.push('.'),
            Some(mut c) => {
                if c as u32 == escape {
                    let next = pcs.next().map_or(escape as u8, |&c| u8::from(c));
                    c = next;
                }
                let mut s = String::new();
                s.push(c as char);
                res.push_str(&regex::escape(&s));
            }
            None => break,
        };
    }
    res.push('$');
    let reg = regex::Regex::new(&res).unwrap();

    Ok(reg.is_match(std::str::from_utf8(target).unwrap()))
}
