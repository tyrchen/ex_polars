---
marp: true
title: 'ExPolars Lessons Learned'
paginate: false
_paginate: false
theme: uncover
---

<!-- backgroundColor: #F7F8F8 -->

![bg](images/tubi.png)
![](#fff)

##
##
##
##
##
##
##
##

### ExPolars Lessons Learned

---

## The Vision: Bring the beauty of _pandas_ into Elixir world

---

## The plan

- use [polars](https://github.com/ritchie46/polars) for data manipulation
- use [vega-lite](https://vega.github.io/vega-lite/) for data visualization

---

## The tools / pre-knowledge

- basic understanding of rust
- basic understanding how erlang NIF works
- rustler: bridge elixir/rust

---

## Meet the brick walls

> The brick walls aren't there to keep us out, they're there to give us a chance to _show how badly we want something._
>
> by Randy Pausch

---

<!-- _backgroundColor: #ffffed -->
## Brick wall #1: Rust compiler won't let you DRY!

```rust
// the dataframe passed between elixir and rust is a ResourceArc<RwLock<DataFrame>>
// take read lock and process
match data.inner.0.read() {
  Ok(df) => deal_with_df,
  Err(_) => Err(ExPolarsError.Internal)
}

// I want to DRY, but this won't work.
def get_reader(*self) -> DataFrame {
  match data.inner.0.read() {
    Ok(df) => (&*df).as_ref(),
    Err(_) => Err(ExPolarsError.Internal)
  }
}

let df = data.inner.0.read().map_err(...)?; // won't work
```

---

## Reason and Solution

- Reason: `Result<RwLockReadGuard<T>, PoisonError<RwLockReadGuard<T>>>` is a RAII structure
  - you can't access it in one function, and utilize the data in another function
  - it didn't implement `sync` so you can't do `map_err`
- Solution: use macros

```rust
macro_rules! df_read {
    ($data: ident, $df: ident, $body: block) => {
        match $data.inner.0.read() {
            Ok($df) => $body,
            Err(_) => Err(ExPolarsError::Internal),
        }
    };
}
```

---

<!-- _color: white -->

![bg](images/solved.webp)

## Problem solved

---

## Brick wall #2: call elixir code in rust

- `groupby_apply(data: ExDataFrame, by: Vec<&str>, lambda: Fun) -> Result<ExDataFrame, ExPolarsError>`
- I don't know how to execute a user passed elixir function in rust context
  - rustler didn't provide the functionality - yes! OSS-a-thon moment!
  - however to build it is not easy: I need to wrap the erlang runtime in rust side
- Solution: add a nice `TODO(tchen)`

---

## Brick wall #3: I want to export more than 1 modules

- I built `DataFrame` and `Series`, I want to export it to `Elixir.ExPolars.DataFrame` and `Elixir.ExPolars.Series`
  - this is a pretty legitimate ask, right?
- `rustler::init!()` can only run once
  - heartbroken moment
  - it actually makes sense: an erlang module loads the ".so", you don't want the single ".so" being loaded multiple times by multiple modules
- Solution: workaround it
  - export to `Elixir.ExPolars.Native`, and prefix the functions with `df_` and `s_`
  - write `Elixir.ExPolars.DataFrame` and `Elixir.ExPolars.Series` to proxy to the native functions

---
## Brick wall #4: Convert data types

- Problem: I need to pass the data as a list back to elixir
  - that involves Arrow type to elixir type conversion
  - Not able to find an elegant way during the hackathon, so decided to use JSON
  - However Arrow types didn't implement Serialize/Deserialize
- Solution:
  - Convert Arrow type to primitive type
  - serde_json::to_string()

---

## Brick wall #5: macro_rules! and procedure_macro problem

```rust
macro_rules! impl_cmp {
    ($name:ident, $type:ty, $operand:ident) => {
        #[rustler::nif] // <- this line will have issue
        pub fn $name(data: ExSeries, rhs: $type) -> Result<ExSeries, ExPolarsError> {
            let s = &data.inner.0;
            Ok(ExSeries::new(s.$operand(rhs).into_series()))
        }
    };
}

impl_cmp!(s_eq_u8, u8, eq);
```

## Solution

```rust
macro_rules! impl_cmp_u8 {
    ($name:ident, $operand:ident) => {
        #[rustler::nif]
        pub fn $name(data: ExSeries, rhs: u8) -> Result<ExSeries, ExPolarsError> {
            let s = &data.inner.0;
            Ok(ExSeries::new(s.$operand(rhs).into_series()))
        }
    };
}
impl_cmp_u8!(s_eq_u8, eq);
```

---
<!-- _backgroundColor: darkgrey -->
<!-- _color: white -->

# May the _Rust_ be with you!

![bg left](images/happy.webp)
