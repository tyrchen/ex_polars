// This file is modified based on: https://github.com/ritchie46/polars/blob/master/py-polars/src/datatypes.rs

use std::sync::RwLock;
use polars::prelude::*;
use rustler::NifStruct;
use rustler::resource::ResourceArc;

use std::result::Result;


pub struct ExDataFrameRef(pub RwLock<DataFrame>);
pub struct ExSeriesRef(pub Series);

#[derive(NifStruct)]
#[module = "ExPolars.DataFrame"]
pub struct ExDataFrame {
    pub inner: ResourceArc<ExDataFrameRef>,
}

#[derive(NifStruct)]
#[module = "ExPolars.Series"]
pub struct ExSeries {
    pub inner: ResourceArc<ExSeriesRef>,
}

// Don't change the order of these!
#[repr(u8)]
pub enum DataType {
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Bool,
    Utf8,
    List,
    Date32,
    Date64,
    Time64Nanosecond,
    DurationNanosecond,
    DurationMillisecond,
    Object,
}

impl ExDataFrameRef {
    pub fn new(df: DataFrame) -> Self {
        Self(RwLock::new(df))
    }
}

impl ExSeriesRef {
    pub fn new(s: Series) -> Self {
        Self(s)
    }
}

impl ExDataFrame {
    pub fn new(df: DataFrame) -> Self {
        Self {
            inner: ResourceArc::new(ExDataFrameRef::new(df))
        }
    }
}


impl ExSeries {
    pub fn new(s: Series) -> Self {
        Self {
            inner: ResourceArc::new(ExSeriesRef::new(s))
        }
    }
}

impl From<&ArrowDataType> for DataType {
    fn from(dt: &ArrowDataType) -> Self {
        use DataType::*;
        match dt {
            ArrowDataType::Int8 => Int8,
            ArrowDataType::Int16 => Int16,
            ArrowDataType::Int32 => Int32,
            ArrowDataType::Int64 => Int64,
            ArrowDataType::UInt8 => UInt8,
            ArrowDataType::UInt16 => UInt16,
            ArrowDataType::UInt32 => UInt32,
            ArrowDataType::UInt64 => UInt64,
            ArrowDataType::Float32 => Float32,
            ArrowDataType::Float64 => Float64,
            ArrowDataType::Boolean => Bool,
            ArrowDataType::Utf8 => Utf8,
            ArrowDataType::List(_) => List,
            ArrowDataType::Date32(_) => Date32,
            ArrowDataType::Date64(_) => Date64,
            ArrowDataType::Time64(TimeUnit::Nanosecond) => Time64Nanosecond,
            ArrowDataType::Duration(TimeUnit::Nanosecond) => DurationNanosecond,
            ArrowDataType::Duration(TimeUnit::Millisecond) => DurationMillisecond,
            ArrowDataType::Binary => Object,
            dt => panic!(format!("datatype: {:?} not supported", dt)),
        }
    }
}
