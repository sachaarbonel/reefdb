use super::{tokenizers::default::DefaultTokenizer, disk::OnDiskInvertedIndex};
use crate::indexes::gin::GinIndex;

pub type DefaultSearchIdx = GinIndex<DefaultTokenizer>;

pub type OnDiskSearchIdx = OnDiskInvertedIndex<DefaultTokenizer>;
