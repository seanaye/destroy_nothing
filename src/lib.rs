use itertools::{EitherOrBoth, Itertools};
use std::hash::{DefaultHasher, Hash, Hasher};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug)]
pub enum DestroyNothingError {
    /// the top level json structure is not an
    /// object or an array
    NotAContainer,
    /// The outer container was modified
    ContainerMismatch,
    Serde(serde_json::Error),
}

pub struct Access<T> {
    extracted: T,
    raw_json: Value,
}

impl From<serde_json::Error> for DestroyNothingError {
    fn from(value: serde_json::Error) -> Self {
        DestroyNothingError::Serde(value)
    }
}

trait Diff<'a, Rhs = Self> {
    fn diff<D>(&self, other: &'a Rhs, hook: &mut D) -> Result<(), DestroyNothingError>
    where
        D: DiffHook<'a>;
}

#[derive(Debug, Clone, Copy)]
enum Key<'a> {
    Object(&'a str),
    Array(usize),
}

#[derive(Debug, Clone, Default)]
struct NodePath<'a> {
    inner: Vec<Key<'a>>,
}

#[derive(Debug, Clone, Copy)]
enum Existence {
    Undefined,
    Defined { hash: u64 },
}

impl<'a> NodePath<'a> {
    fn new(start: Key<'a>) -> Self {
        Self { inner: vec![start] }
    }

    fn segments(&'a self) -> impl Iterator<Item = Key<'a>> + 'a {
        self.inner.iter().copied()
    }

    fn new_append(&self, to_add: Key<'a>) -> Self {
        let mut next = self.inner.clone();
        next.push(to_add);
        Self { inner: next }
    }

    fn slice_to(&'a self, end: usize) -> KeySlice<'a> {
        let s = &self.inner[0..end];
        KeySlice { inner: s }
    }

    fn slice(&'a self) -> KeySlice<'a> {
        KeySlice {
            inner: self.inner.as_slice(),
        }
    }
}

struct KeySlice<'a> {
    inner: &'a [Key<'a>],
}

impl<'a> std::fmt::Display for KeySlice<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for segment in Itertools::intersperse(self.inner.iter(), &Key::Object("/")) {
            match segment {
                Key::Object(x) => {
                    write!(f, "{x}")?;
                }
                Key::Array(x) => {
                    write!(f, "{x}")?;
                }
            }
        }
        Ok(())
    }
}

impl<'a> KeySlice<'a> {
    fn to_pointer(&self) -> String {
        self.to_string()
    }
}

#[derive(Debug)]
enum Op<'a> {
    /// a new child node was added
    Add { path: NodePath<'a>, data: &'a Value },
    /// a child node was removed
    /// we compute the hash of the node to be removed
    /// to make absolutely sure that we want to remove it
    Remove { path: NodePath<'a>, hash: u64 },
    /// the contents of a child node was changed
    Replace {
        path: NodePath<'a>,
        data: &'a Value,
        hash: u64,
    },
}

trait DiffHook<'a> {
    /// appends a new Op into self
    fn add_op(&mut self, op: Op<'a>);

    /// replay the [Op] in the order they should be applied
    fn replay(self) -> impl Iterator<Item = Op<'a>>;
}

trait ApplyPatch {
    fn apply_patch<'a, T>(&'a mut self, hook: T) -> Vec<MergingError>
    where
        T: DiffHook<'a>,
    {
        hook.replay()
            .filter_map(|op| match self.apply_op(op) {
                Err(m) => Some(m),
                Ok(()) => None,
            })
            .collect()
    }

    fn apply_op(&mut self, op: Op<'_>) -> Result<(), MergingError>;
}

enum MergingError {
    /// this JSON pointer cant be found in the targe object
    PathNotFound(String),
    /// this value found at this path was unexpected
    UnexpectedValue { path: String, expected: Existence },
    /// the hash of this value does not match
    HashMismatch {
        path: String,
        expected: u64,
        found: u64,
    },
}

fn val_at_key_slice_mut<'a>(
    path: &NodePath<'_>,
    val: &'a mut Value,
    reverse_index: usize,
) -> Result<&'a mut Value, MergingError> {
    let idx = path.inner.len().saturating_sub(reverse_index);
    let ptr = path.slice_to(idx).to_pointer();
    val.pointer_mut(&ptr).ok_or(MergingError::PathNotFound(ptr))
}

/// attempts to replace this path on the passed [Value]
/// if the key or index at the last position does not yet exist
/// it will be created before filling.
/// If there are intermediate keys that are missing then this function
/// will return an error. Only the key in the last position is allowed to be missing
/// expect undefined will cause this function to error if a value is already defined at the target position
fn replace_value_mut(
    path: &NodePath<'_>,
    val: &mut Value,
    replace_with: &Value,
    expect: Existence,
) -> Result<(), MergingError> {
    let mut_val = val_at_key_slice_mut(path, val, 1)?;
    match (mut_val, path.inner.last()) {
        (Value::Array(a), Some(Key::Array(n))) => match (expect, a.get(*n)) {
            (Existence::Undefined, Some(_)) | (Existence::Defined { .. }, None) => {
                return Err(MergingError::UnexpectedValue {
                    path: path.slice().to_pointer(),
                    expected: expect,
                });
            }

            (Existence::Defined { hash }, Some(x)) => {
                let this_hash = hash_val(x);
                match hash == this_hash {
                    true => {
                        a.insert(*n, replace_with.clone());
                    }
                    false => {
                        return Err(MergingError::HashMismatch {
                            path: path.slice().to_pointer(),
                            expected: hash,
                            found: this_hash,
                        })
                    }
                }
            }
            (Existence::Undefined, None) => {
                a.insert(*n, replace_with.clone());
            }
        },
        (Value::Object(o), Some(Key::Object(s))) => match (expect, o.get(*s)) {
            (Existence::Undefined, Some(_)) | (Existence::Defined { .. }, None) => {
                return Err(MergingError::UnexpectedValue {
                    path: path.slice().to_pointer(),
                    expected: expect,
                });
            }
            (Existence::Defined { hash }, Some(x)) => {
                let this_hash = hash_val(x);
                match hash == this_hash {
                    true => {
                        o.insert(s.to_string(), replace_with.clone());
                    }
                    false => {
                        return Err(MergingError::HashMismatch {
                            path: path.slice().to_pointer(),
                            expected: hash,
                            found: this_hash,
                        })
                    }
                }
            }
            (Existence::Undefined, None) => {
                o.insert(s.to_string(), replace_with.clone());
            }
        },
        _ => return Err(MergingError::PathNotFound(path.slice().to_pointer())),
    };
    Ok(())
}

impl ApplyPatch for Value {
    fn apply_op(&mut self, op: Op<'_>) -> Result<(), MergingError> {
        match op {
            Op::Add { path, data } => replace_value_mut(&path, self, data, Existence::Undefined),
            Op::Remove { path, hash } => {
                let val = val_at_key_slice_mut(&path, self, 0)?;
                let this_hash = hash_val(val);
                if hash != this_hash {
                    return Err(MergingError::HashMismatch {
                        path: path.slice().to_pointer(),
                        expected: hash,
                        found: this_hash,
                    });
                }

                match (val_at_key_slice_mut(&path, self, 1)?, path.inner.last()) {
                    (Value::Array(a), Some(Key::Array(n))) => {
                        a.remove(*n);
                        Ok(())
                    }
                    (Value::Object(o), Some(Key::Object(k))) => {
                        o.remove(*k);
                        Ok(())
                    }
                    _ => unreachable!("We already indexed beyond this value"),
                }
            }
            Op::Replace { path, data, hash } => {
                replace_value_mut(&path, self, data, Existence::Defined { hash })
            }
        }
    }
}

#[derive(Debug, Default)]
struct DiffHookImpl<'a> {
    ops: Vec<Op<'a>>,
}

impl<'a> DiffHook<'a> for DiffHookImpl<'a> {
    fn add_op(&mut self, op: Op<'a>) {
        self.ops.push(op)
    }

    fn replay(self) -> impl Iterator<Item = Op<'a>> {
        self.ops.into_iter()
    }
}

/// wrapper around [Value] which keeps track of its path
struct ValuePath<'a> {
    value: &'a Value,
    path: NodePath<'a>,
}

impl<'a> ValuePath<'a> {
    pub fn root(value: &'a Value) -> Self {
        Self {
            value,
            path: NodePath::default(),
        }
    }
}

/// internal helper function for hashing json
fn hash_val(val: &Value) -> u64 {
    let mut hasher = DefaultHasher::new();
    val.hash(&mut hasher);
    hasher.finish()
}

impl<'a> Diff<'a, Value> for ValuePath<'a> {
    fn diff<D>(&self, new: &'a Value, hook: &mut D) -> Result<(), DestroyNothingError>
    where
        D: DiffHook<'a>,
    {
        let replace = |old: &'a Value, new: &'a Value, hook: &mut D| {
            hook.add_op(Op::Replace {
                path: self.path.clone(),
                data: new,
                hash: hash_val(old),
            });
        };

        match (&self.value, &new) {
            (Value::Null, Value::Null) => {}
            (
                Value::Null,
                Value::Bool(_)
                | Value::Number(_)
                | Value::String(_)
                | Value::Array(_)
                | Value::Object(_),
            ) => replace(self.value, new, hook),
            (Value::Bool(_), Value::Bool(_)) => {}
            (
                Value::Bool(_),
                Value::Null
                | Value::Number(_)
                | Value::String(_)
                | Value::Array(_)
                | Value::Object(_),
            ) => replace(self.value, new, hook),
            (Value::Number(_), Value::Number(_)) => {}
            (
                Value::Number(_),
                Value::Null
                | Value::Bool(_)
                | Value::String(_)
                | Value::Array(_)
                | Value::Object(_),
            ) => replace(self.value, new, hook),
            (Value::String(_), Value::String(_)) => {}
            (
                Value::String(_),
                Value::Null
                | Value::Bool(_)
                | Value::Number(_)
                | Value::Array(_)
                | Value::Object(_),
            ) => replace(self.value, new, hook),
            (Value::Array(old_arr), Value::Array(new_arr)) => {
                for (idx, res) in old_arr.iter().zip_longest(new_arr).enumerate() {
                    let path = self.path.new_append(Key::Array(idx));
                    match res {
                        EitherOrBoth::Left(old) => {
                            // the old value has this index but the new value does not
                            hook.add_op(Op::Remove {
                                path,
                                hash: hash_val(old),
                            })
                        }
                        EitherOrBoth::Both(old, new) => {
                            let old_val = ValuePath { value: old, path };

                            old_val.diff(new, hook)?
                        }
                        EitherOrBoth::Right(x) => {
                            // the new value has an this index but the old value does not
                            hook.add_op(Op::Add { path, data: x })
                        }
                    }
                }
            }
            (
                Value::Array(_),
                Value::Null
                | Value::Bool(_)
                | Value::Number(_)
                | Value::String(_)
                | Value::Object(_),
            ) => return Err(DestroyNothingError::ContainerMismatch),
            (Value::Object(old_obj), Value::Object(new_obj)) => {
                for key in old_obj.summary(&new_obj) {
                    let path = self.path.new_append(Key::Object(key.as_ref()));
                    match key {
                        // left only means deleted in new obj
                        CompareKeys::Left(_, v) => hook.add_op(Op::Remove {
                            path,
                            hash: hash_val(v),
                        }),
                        // inner is common between obj
                        CompareKeys::Inner { left, right, .. } => {
                            let old_val = ValuePath { value: left, path };
                            old_val.diff(right, hook)?
                        }
                        // right is newly added
                        CompareKeys::Right(_, v) => hook.add_op(Op::Add { path, data: v }),
                    }
                }
            }
            (
                Value::Object(_),
                Value::Null
                | Value::Bool(_)
                | Value::Number(_)
                | Value::String(_)
                | Value::Array(_),
            ) => return Err(DestroyNothingError::ContainerMismatch),
        };
        Ok(())
    }
}

impl<T> Access<T> {
    pub fn new<'a>(s: &'a str) -> Result<Access<T>, DestroyNothingError>
    where
        T: Deserialize<'a>,
    {
        let raw_json: Value = serde_json::from_str(s)?;
        let extracted: T = serde_json::from_str(s)?;

        if !matches!(raw_json, Value::Object(_)) {
            return Err(DestroyNothingError::NotAContainer);
        }

        Ok(Access {
            extracted,
            raw_json,
        })
    }

    pub fn diff(self, new: T) -> Result<MergeSummary, DestroyNothingError>
    where
        T: Serialize,
    {
        let old_entries = serde_json::value::to_value(self.extracted)?;
        let new_entries = serde_json::value::to_value(new)?;

        let cur = ValuePath::root(&old_entries);
        let mut hook = DiffHookImpl::default();
        cur.diff(&new_entries, &mut hook)?;
        let mut to_modify = self.raw_json;
        let errors = to_modify.apply_patch(hook);

        Ok(MergeSummary {
            value: to_modify,
            errors,
        })
    }
}

struct MergeSummary {
    pub value: Value,
    pub errors: Vec<MergingError>,
}

enum CompareKeys<K, V> {
    /// this key is only present in the left collection
    Left(K, V),
    /// this key is shared between both collections (inner join)
    Inner { left: V, right: V, key: K },
    /// this key is only present in the right collection
    Right(K, V),
}

impl<T, U> AsRef<T> for CompareKeys<T, U> {
    fn as_ref(&self) -> &T {
        match self {
            CompareKeys::Left(x, _)
            | CompareKeys::Inner { key: x, .. }
            | CompareKeys::Right(x, _) => x,
        }
    }
}

trait KeySummary<K, V> {
    fn summary(&self, other: &Self) -> Vec<CompareKeys<K, V>>;
}

impl<'a> KeySummary<&'a str, &'a Value> for &'a Map<String, Value> {
    fn summary(&self, other: &Self) -> Vec<CompareKeys<&'a str, &'a Value>> {
        let mut output = Vec::new();
        for (left_key, left_value) in self.iter() {
            match other.get(left_key) {
                Some(right_value) => output.push(CompareKeys::Inner {
                    left: left_value,
                    right: right_value,
                    key: left_key.as_str(),
                }),
                None => output.push(CompareKeys::Left(left_key, left_value)),
            };
        }
        for (right_key, right_value) in other.iter() {
            if let None = self.get(right_key) {
                output.push(CompareKeys::Right(right_key, right_value))
            }
        }
        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    static item_a: &'static str = r#"{
  "overview": {
    "title": "Server",
    "ainfo": "username"
  },
  "details": {
    "sections": [
      {
        "name": "",
        "title": "",
        "fields": [
          {
            "t": "URL",
            "n": "url",
            "k": "string",
            "inputTraits": {
              "keyboard": "URL"
            }
          },
          {
            "t": "username",
            "n": "username",
            "k": "string",
            "inputTraits": {
              "autocorrection": "no",
              "autocapitalization": "none"
            },
            "v": "username"
          },
          {
            "t": "password",
            "n": "password",
            "k": "concealed",
            "v": "password"
          },
          {
            "t": "password",
            "n": "v4pljk3gpw4mg7l2nnwazhiyvm",
            "k": "concealed",
            "v": "secondpassword",
            "inputTraits": {
              "autocorrection": "no",
              "autocapitalization": "none"
            }
          }
        ]
      },
      {
        "name": "admin_console",
        "title": "Admin Console",
        "fields": [
          {
            "t": "admin console URL",
            "n": "admin_console_url",
            "k": "string",
            "inputTraits": {
              "keyboard": "URL"
            }
          },
          {
            "t": "admin console username",
            "n": "admin_console_username",
            "k": "string",
            "inputTraits": {
              "autocorrection": "no",
              "autocapitalization": "none"
            }
          },
          {
            "t": "console password",
            "n": "admin_console_password",
            "k": "concealed"
          }
        ]
      },
      {
        "name": "hosting_provider_details",
        "title": "Hosting Provider",
        "fields": [
          {
            "t": "name",
            "n": "name",
            "k": "string",
            "inputTraits": {
              "autocapitalization": "Words"
            }
          },
          {
            "t": "website",
            "n": "website",
            "k": "string",
            "inputTraits": {
              "keyboard": "URL"
            }
          },
          {
            "t": "support URL",
            "n": "support_contact_url",
            "k": "string",
            "inputTraits": {
              "keyboard": "URL"
            }
          },
          {
            "t": "support phone",
            "n": "support_contact_phone",
            "k": "string",
            "inputTraits": {
              "keyboard": "NamePhonePad"
            }
          }
        ]
      }
    ]
  },
  "createdAt": "2024-09-06T14:12:21Z",
  "updatedAt": "2024-09-06T17:23:47Z",
  "faveIndex": 0,
  "trashed": "N",
  "templateUuid": "110",
  "uuid": "3sxlf7itisprwr63rs2nkzhpi4"
}"#;

    static item_b: &'static str = r#"{
  "overview": {
    "title": "Login",
    "ainfo": "test",
    "ps": 6,
    "url": "https://*.google.com",
    "URLs": [
      {
        "u": "https://*.google.com",
        "l": "website",
        "m": "default"
      }
    ]
  },
  "details": {
    "fields": [
      {
        "value": "test",
        "id": "",
        "name": "username",
        "type": "T",
        "designation": "username"
      },
      {
        "value": "test1",
        "id": "",
        "name": "password",
        "type": "P",
        "designation": "password"
      }
    ],
    "notesPlain": "something",
    "passwordHistory": [
      {
        "value": "test",
        "time": 1719515490
      },
      {
        "value": "test",
        "time": 1719515513
      },
      {
        "value": "test",
        "time": 1719516194
      },
      {
        "value": "test",
        "time": 1719521497
      }
    ]
  },
  "createdAt": "2024-04-09T21:50:33Z",
  "updatedAt": "2024-06-27T20:51:37Z",
  "faveIndex": 0,
  "trashed": "N",
  "templateUuid": "001",
  "uuid": "qmutr7cwhhgln2yhhah2qs6uy4"
}"#;

    #[test]
    fn it_should_compare() {
        let access: Access<Value> = Access::new(item_a).unwrap();

        let other: Value = serde_json::from_str(item_b).unwrap();

        access.diff(other);
        panic!("test");
    }
}
